"""
Multi-source -> S3 sync worker with live web dashboard.
Sources:
  1. Kaggle: ammarshafiq/supreme-court-of-pakistan-judgments-dataset  (PDFs)
  2. Google Drive file ID: 1eLzYK2cPQUZqOqtpGLBNLmwMRtJoQPJr         (Markdown, 1970-2025)
"""

import os
import sys
import json
import time
import zipfile
import logging
import hashlib
import tempfile
import threading
from pathlib import Path
from datetime import datetime, timezone

import boto3
import gdown
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from flask import Flask, jsonify, Response

load_dotenv()

# ---------- Config -----------------------------------------------------------

KAGGLE_DATASET   = "ammarshafiq/supreme-court-of-pakistan-judgments-dataset"
KAGGLE_S3_PREFIX = os.getenv("S3_PREFIX", "supreme-court-pakistan/")

GDRIVE_FILE_ID   = os.getenv("GDRIVE_FILE_ID", "1eLzYK2cPQUZqOqtpGLBNLmwMRtJoQPJr")
GDRIVE_S3_PREFIX = os.getenv("GDRIVE_S3_PREFIX", "judgments-markdown/")

S3_BUCKET        = os.getenv("S3_BUCKET_NAME", "assancase-documents-adminpenta")
AWS_REGION       = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
MANIFEST_KEY     = "sync-manifest.json"   # unified manifest covering both sources
POLL_INTERVAL    = int(os.getenv("POLL_INTERVAL_SECONDS", "1"))
# Force port 8080 as Railway domain is explicitly mapped to 8080
PORT             = 8080
WORK_DIR         = Path(os.getenv("WORK_DIR", tempfile.gettempdir())) / "kaggle_s3"
MAX_ACTIVITY     = 300

# ---------- Shared state (worker writes, Flask reads) ------------------------

state = {
    "phase":        "starting",
    "sources": {
        "kaggle": {"status": "pending", "total": 0, "uploaded": 0, "skipped": 0, "errors": 0},
        "gdrive": {"status": "pending", "total": 0, "uploaded": 0, "skipped": 0, "errors": 0},
    },
    "total":        0,
    "uploaded":     0,
    "skipped":      0,
    "errors":       0,
    "pending":      0,
    "tick":         0,
    "elapsed_s":    0,
    "start_time":   None,
    "error_msg":    "",
    "activity":     [],
    "uploaded_files":  [],
    "skipped_files":   [],
    "error_files":     [],
}
_lock = threading.Lock()

log = logging.getLogger("worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

# ---------- State helpers ----------------------------------------------------

def set_state(**kw):
    with _lock:
        state.update(kw)

def set_source(name, **kw):
    with _lock:
        state["sources"][name].update(kw)

def push_activity(kind: str, name: str, size_kb: float = 0, source: str = ""):
    entry = {
        "type":    kind,
        "name":    name,
        "size_kb": round(size_kb, 1),
        "source":  source,
        "ts":      datetime.now(timezone.utc).strftime("%H:%M:%S"),
    }
    with _lock:
        state["activity"].insert(0, entry)
        if len(state["activity"]) > MAX_ACTIVITY:
            state["activity"].pop()
        if kind == "uploaded":
            state["uploaded_files"].append(name)
        elif kind == "skipped":
            state["skipped_files"].append(name)
        elif kind == "error":
            state["error_files"].append(name)

# ---------- AWS helpers ------------------------------------------------------

def make_s3():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def verify_bucket(s3):
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            raise PermissionError(f"Access denied to bucket '{S3_BUCKET}'.")
        raise

def load_manifest(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise

def save_manifest(s3, manifest: dict):
    body = json.dumps(manifest, indent=2).encode()
    s3.put_object(Bucket=S3_BUCKET, Key=MANIFEST_KEY, Body=body,
                  ContentType="application/json")

# ---------- File helpers -----------------------------------------------------

def md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()

def size_kb(path: Path) -> float:
    try:
        return path.stat().st_size / 1024
    except Exception:
        return 0.0

def content_type(path: Path) -> str:
    return {
        ".md":   "text/markdown",
        ".csv":  "text/csv",
        ".json": "application/json",
        ".pdf":  "application/pdf",
        ".txt":  "text/plain",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(path.suffix.lower(), "application/octet-stream")

def extract_zip(zip_path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)

def build_index(root: Path, s3_prefix: str) -> dict[str, Path]:
    return {
        s3_prefix + f.relative_to(root).as_posix(): f
        for f in root.rglob("*") if f.is_file()
    }

# ---------- Kaggle download --------------------------------------------------

def setup_kaggle():
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    token    = os.getenv("KAGGLE_TOKEN")
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")
    if token:
        os.environ["KAGGLE_API_TOKEN"] = token
        (kaggle_dir / "access_token").write_text(token, encoding="utf-8")
    elif username and key:
        (kaggle_dir / "kaggle.json").write_text(
            json.dumps({"username": username, "key": key}), encoding="utf-8"
        )
    else:
        raise EnvironmentError("Set KAGGLE_TOKEN or KAGGLE_USERNAME + KAGGLE_KEY.")

def download_kaggle() -> Path:
    from kaggle import KaggleApi
    api = KaggleApi()
    api.authenticate()

    zip_dir = WORK_DIR / "kaggle" / "raw"
    zip_dir.mkdir(parents=True, exist_ok=True)
    extract_dir = WORK_DIR / "kaggle" / "extracted"

    if extract_dir.exists() and any(extract_dir.rglob("*.pdf")):
        log.info("Kaggle: using cached extraction at %s", extract_dir)
        return extract_dir

    set_source("kaggle", status="downloading")
    log.info("Kaggle: downloading dataset ...")
    api.dataset_download_files(KAGGLE_DATASET, path=str(zip_dir), unzip=False, quiet=False)

    zips = list(zip_dir.glob("*.zip"))
    if not zips:
        raise FileNotFoundError("Kaggle: no zip found after download.")

    set_source("kaggle", status="extracting")
    for z in zips:
        log.info("Kaggle: extracting %s ...", z.name)
        extract_zip(z, extract_dir)

    return extract_dir

# ---------- Google Drive download --------------------------------------------

def download_gdrive() -> Path:
    extract_dir = WORK_DIR / "gdrive" / "extracted"

    if extract_dir.exists() and any(extract_dir.rglob("*.md")):
        log.info("GDrive: using cached extraction at %s", extract_dir)
        return extract_dir

    zip_dir = WORK_DIR / "gdrive" / "raw"
    zip_dir.mkdir(parents=True, exist_ok=True)
    zip_path = zip_dir / "gdrive_download.zip"

    set_source("gdrive", status="downloading")
    log.info("GDrive: downloading file %s ...", GDRIVE_FILE_ID)
    url = f"https://drive.google.com/uc?id={GDRIVE_FILE_ID}"
    result = gdown.download(url, str(zip_path), quiet=False)
    if not result:
        raise RuntimeError("GDrive: download returned None — file may be private or deleted.")

    set_source("gdrive", status="extracting")
    log.info("GDrive: extracting %s ...", zip_path.name)
    extract_zip(zip_path, extract_dir)

    return extract_dir

# ---------- Upload loop ------------------------------------------------------

def upload_loop(s3, file_index: dict[str, Path], source_map: dict[str, str]):
    """
    Poll every POLL_INTERVAL seconds.
    Upload any file whose md5 differs from the manifest.
    Exit when all files are confirmed in S3.
    source_map: {s3_key -> source_name} for dashboard tagging.
    """
    total = len(file_index)
    set_state(phase="uploading", total=total)
    log.info("Upload loop started — %d total files, polling every %ds.", total, POLL_INTERVAL)

    tick = 0
    start = datetime.now(timezone.utc)

    while True:
        tick += 1
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        manifest = load_manifest(s3)

        uploaded_count = skipped_count = pending_count = error_count = 0
        pending = {}

        # Tally skipped/pending per file
        known_skipped = set(state["skipped_files"])
        for key, path in file_index.items():
            checksum = md5(path)
            if manifest.get(key) == checksum:
                skipped_count += 1
                name = key.split("/", 1)[-1] if "/" in key else key
                if name not in known_skipped:
                    push_activity("skipped", name, size_kb(path), source_map.get(key, ""))
            else:
                pending[key] = (path, checksum)
                pending_count += 1

        uploaded_count = total - skipped_count - pending_count

        # Per-source counters
        for src_name in ("kaggle", "gdrive"):
            src_keys = [k for k, v in source_map.items() if v == src_name]
            src_up   = sum(1 for k in src_keys if manifest.get(k) == md5(file_index[k]) and k not in pending)
            src_skip = sum(1 for k in src_keys if manifest.get(k) == md5(file_index[k]) and k not in pending)
            set_source(src_name,
                       total=len(src_keys),
                       uploaded=uploaded_count,
                       skipped=skipped_count,
                       status="uploading" if pending_count else "complete")

        set_state(
            tick=tick,
            elapsed_s=round(elapsed),
            uploaded=uploaded_count,
            skipped=skipped_count,
            pending=pending_count,
        )

        if not pending:
            save_manifest(s3, manifest)
            elapsed_final = (datetime.now(timezone.utc) - start).total_seconds()
            set_state(phase="complete", elapsed_s=round(elapsed_final),
                      uploaded=uploaded_count, skipped=skipped_count,
                      pending=0, errors=0)
            for src in ("kaggle", "gdrive"):
                set_source(src, status="complete")
            log.info("All %d files confirmed in S3. Done in %.0fs.", total, elapsed_final)
            break

        # Upload pending files
        errors = 0
        for s3_key, (local, checksum) in pending.items():
            src  = source_map.get(s3_key, "")
            name = s3_key.split("/", 1)[-1] if "/" in s3_key else s3_key
            try:
                s3.upload_file(
                    str(local), S3_BUCKET, s3_key,
                    ExtraArgs={"ContentType": content_type(local)},
                )
                manifest[s3_key] = checksum
                push_activity("uploaded", name, size_kb(local), src)
            except ClientError as e:
                log.error("Error uploading %s: %s", s3_key, e)
                push_activity("error", name, 0, src)
                errors += 1

        with _lock:
            state["errors"] = errors

        save_manifest(s3, manifest)
        time.sleep(POLL_INTERVAL)

# ---------- Worker entry point -----------------------------------------------

def worker():
    start = datetime.now(timezone.utc)
    set_state(phase="starting", start_time=start.isoformat())

    try:
        s3 = make_s3()
        verify_bucket(s3)
        log.info("Bucket verified: s3://%s", S3_BUCKET)

        # -- Source 1: Kaggle --
        setup_kaggle()
        kaggle_dir = download_kaggle()
        kaggle_index = build_index(kaggle_dir, KAGGLE_S3_PREFIX)
        set_source("kaggle", status="ready", total=len(kaggle_index))
        log.info("Kaggle: %d files indexed.", len(kaggle_index))

        # -- Source 2: Google Drive --
        gdrive_dir = download_gdrive()
        gdrive_index = build_index(gdrive_dir, GDRIVE_S3_PREFIX)
        set_source("gdrive", status="ready", total=len(gdrive_index))
        log.info("GDrive: %d files indexed.", len(gdrive_index))

        # -- Merge & tag --
        file_index = {**kaggle_index, **gdrive_index}
        source_map = (
            {k: "kaggle" for k in kaggle_index}
            | {k: "gdrive" for k in gdrive_index}
        )
        log.info("Combined: %d files total.", len(file_index))

        # -- Upload loop --
        upload_loop(s3, file_index, source_map)

    except Exception as exc:
        log.exception("Worker crashed: %s", exc)
        set_state(phase="error", error_msg=str(exc))

# ---------- Flask dashboard --------------------------------------------------

app = Flask(__name__)

@app.route("/status")
def status_api():
    with _lock:
        return jsonify(dict(state))

@app.route("/files/<kind>")
def file_list(kind):
    with _lock:
        data = {
            "uploaded": state["uploaded_files"],
            "skipped":  state["skipped_files"],
            "errors":   state["error_files"],
        }.get(kind, [])
    return jsonify(data)

@app.route("/")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>S3 Upload Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body{background:#0f172a}
  .card{background:#1e293b;border:1px solid #334155}
  .badge-uploaded{background:#166534;color:#bbf7d0}
  .badge-skipped{background:#1e3a5f;color:#93c5fd}
  .badge-error{background:#7f1d1d;color:#fca5a5}
  .badge-kaggle{background:#4c1d95;color:#ddd6fe}
  .badge-gdrive{background:#164e63;color:#a5f3fc}
  .prog-bar{transition:width .5s ease}
  ::-webkit-scrollbar{width:5px}
  ::-webkit-scrollbar-track{background:#0f172a}
  ::-webkit-scrollbar-thumb{background:#475569;border-radius:3px}
</style>
</head>
<body class="text-slate-200 min-h-screen p-5 font-mono text-sm">
<div class="max-w-6xl mx-auto space-y-4">

  <!-- Header -->
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-lg font-bold text-white">Pakistan Courts &rarr; S3 Dashboard</h1>
      <p class="text-slate-500 text-xs">s3://assancase-documents-adminpenta</p>
    </div>
    <div class="flex items-center gap-2">
      <span id="phase-dot" class="w-2.5 h-2.5 rounded-full bg-yellow-400 animate-pulse"></span>
      <span id="phase-label" class="text-xs uppercase tracking-widest text-slate-300">starting</span>
    </div>
  </div>

  <!-- Overall progress -->
  <div class="card rounded-xl p-5">
    <div class="flex justify-between text-xs text-slate-400 mb-2">
      <span id="prog-text">Initialising...</span>
      <span id="prog-pct">0%</span>
    </div>
    <div class="w-full bg-slate-700 rounded-full h-3 overflow-hidden">
      <div id="prog-bar" class="prog-bar h-3 rounded-full bg-gradient-to-r from-emerald-500 to-teal-400" style="width:0%"></div>
    </div>
    <div class="flex justify-between text-xs text-slate-500 mt-2">
      <span id="elapsed">Elapsed: --</span>
      <span id="tick-info">Tick: 0</span>
    </div>
  </div>

  <!-- Source cards -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    <div class="card rounded-xl p-4">
      <div class="flex items-center gap-2 mb-3">
        <span class="text-xs px-2 py-0.5 rounded badge-kaggle">Kaggle</span>
        <span class="text-xs text-slate-400">Supreme Court PDFs</span>
        <span id="kaggle-status" class="ml-auto text-xs text-slate-500">pending</span>
      </div>
      <div class="grid grid-cols-3 gap-2 text-center">
        <div><div class="text-lg font-bold text-emerald-400" id="kaggle-uploaded">0</div><div class="text-xs text-slate-500">uploaded</div></div>
        <div><div class="text-lg font-bold text-blue-400"    id="kaggle-skipped">0</div> <div class="text-xs text-slate-500">skipped</div></div>
        <div><div class="text-lg font-bold text-slate-400"   id="kaggle-total">0</div>   <div class="text-xs text-slate-500">total</div></div>
      </div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="flex items-center gap-2 mb-3">
        <span class="text-xs px-2 py-0.5 rounded badge-gdrive">Google Drive</span>
        <span class="text-xs text-slate-400">Judgments 1970-2025 (MD)</span>
        <span id="gdrive-status" class="ml-auto text-xs text-slate-500">pending</span>
      </div>
      <div class="grid grid-cols-3 gap-2 text-center">
        <div><div class="text-lg font-bold text-emerald-400" id="gdrive-uploaded">0</div><div class="text-xs text-slate-500">uploaded</div></div>
        <div><div class="text-lg font-bold text-blue-400"    id="gdrive-skipped">0</div> <div class="text-xs text-slate-500">skipped</div></div>
        <div><div class="text-lg font-bold text-slate-400"   id="gdrive-total">0</div>   <div class="text-xs text-slate-500">total</div></div>
      </div>
    </div>
  </div>

  <!-- Total stat row -->
  <div class="grid grid-cols-4 gap-3">
    <div class="card rounded-xl p-4 text-center">
      <div class="text-2xl font-bold text-emerald-400" id="cnt-uploaded">0</div>
      <div class="text-xs text-slate-500 mt-1">Uploaded</div>
    </div>
    <div class="card rounded-xl p-4 text-center">
      <div class="text-2xl font-bold text-blue-400" id="cnt-skipped">0</div>
      <div class="text-xs text-slate-500 mt-1">Skipped</div>
    </div>
    <div class="card rounded-xl p-4 text-center">
      <div class="text-2xl font-bold text-slate-300" id="cnt-pending">0</div>
      <div class="text-xs text-slate-500 mt-1">Pending</div>
    </div>
    <div class="card rounded-xl p-4 text-center">
      <div class="text-2xl font-bold text-red-400" id="cnt-errors">0</div>
      <div class="text-xs text-slate-500 mt-1">Errors</div>
    </div>
  </div>

  <!-- Activity + file list -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    <div class="card rounded-xl p-4">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-3">Live Activity</h2>
      <div id="activity-feed" class="space-y-0.5 max-h-96 overflow-y-auto pr-1"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="flex gap-2 mb-3 flex-wrap">
        <button onclick="showTab('uploaded')" class="text-xs px-3 py-1 rounded-full badge-uploaded">Uploaded</button>
        <button onclick="showTab('skipped')"  class="text-xs px-3 py-1 rounded-full badge-skipped">Skipped</button>
        <button onclick="showTab('errors')"   class="text-xs px-3 py-1 rounded-full badge-error">Errors</button>
      </div>
      <div id="file-list" class="max-h-96 overflow-y-auto space-y-0.5 text-xs text-slate-300 pr-1"></div>
    </div>
  </div>

</div>
<script>
let currentTab = 'uploaded';
let fileCache  = {uploaded:[], skipped:[], errors:[]};

function fmtElapsed(s){
  if(s<60) return s+'s';
  const m=Math.floor(s/60); return m+'m '+(s%60)+'s';
}
function fmtSize(kb){
  if(kb<=0) return '';
  return kb>1024 ? ' '+(kb/1024).toFixed(1)+'MB' : ' '+kb.toFixed(0)+'KB';
}

const phaseConf = {
  starting:    {dot:'bg-yellow-400 animate-pulse', label:'Starting'},
  downloading: {dot:'bg-sky-400 animate-pulse',    label:'Downloading'},
  extracting:  {dot:'bg-violet-400 animate-pulse', label:'Extracting'},
  uploading:   {dot:'bg-emerald-400 animate-pulse',label:'Uploading'},
  complete:    {dot:'bg-emerald-400',              label:'Complete ✓'},
  error:       {dot:'bg-red-500',                  label:'Error'},
};
const srcColors = {kaggle:'badge-kaggle', gdrive:'badge-gdrive'};
const typeColors = {uploaded:'badge-uploaded', skipped:'badge-skipped', error:'badge-error'};

function showTab(kind){ currentTab=kind; renderFileList(); }

function renderFileList(){
  const files = (fileCache[currentTab]||[]).slice().reverse();
  const el = document.getElementById('file-list');
  if(!files.length){el.innerHTML='<div class="text-slate-500 italic">None yet.</div>';return;}
  el.innerHTML = files.map(f=>{
    const short = f.split('/').pop();
    return `<div class="truncate hover:text-white" title="${f}">${short}</div>`;
  }).join('');
}

async function loadFiles(kind){
  try{ const r=await fetch('/files/'+kind); fileCache[kind]=await r.json(); }catch(_){}
}

function renderActivity(items){
  const feed = document.getElementById('activity-feed');
  feed.innerHTML = items.map(e=>{
    const short = e.name.split('/').pop();
    const tc = typeColors[e.type]||'';
    const sc = srcColors[e.source]||'';
    return `<div class="flex items-center gap-1.5 py-0.5">
      <span class="text-slate-600 text-xs w-14 shrink-0">${e.ts}</span>
      <span class="text-xs px-1.5 rounded ${tc} shrink-0">${e.type}</span>
      ${e.source ? `<span class="text-xs px-1 rounded ${sc} shrink-0">${e.source}</span>` : ''}
      <span class="truncate text-xs text-slate-300">${short}${fmtSize(e.size_kb)}</span>
    </div>`;
  }).join('');
}

function setSrcCard(name, s){
  const src = s.sources[name]||{};
  document.getElementById(name+'-status').textContent  = src.status||'--';
  document.getElementById(name+'-uploaded').textContent = (src.uploaded||0).toLocaleString();
  document.getElementById(name+'-skipped').textContent  = (src.skipped||0).toLocaleString();
  document.getElementById(name+'-total').textContent    = (src.total||0).toLocaleString();
}

async function poll(){
  try{
    const r = await fetch('/status');
    const s = await r.json();

    const pc = phaseConf[s.phase]||phaseConf.starting;
    document.getElementById('phase-dot').className   = 'w-2.5 h-2.5 rounded-full '+pc.dot;
    document.getElementById('phase-label').textContent = pc.label;

    const total = s.total||1, done = (s.uploaded||0)+(s.skipped||0);
    const pct = Math.round(done/total*100);
    document.getElementById('prog-bar').style.width  = pct+'%';
    document.getElementById('prog-pct').textContent  = pct+'%';
    document.getElementById('prog-text').textContent =
      done.toLocaleString()+' / '+total.toLocaleString()+' files';
    document.getElementById('elapsed').textContent   = 'Elapsed: '+fmtElapsed(s.elapsed_s||0);
    document.getElementById('tick-info').textContent = 'Tick: '+s.tick;

    document.getElementById('cnt-uploaded').textContent = (s.uploaded||0).toLocaleString();
    document.getElementById('cnt-skipped').textContent  = (s.skipped||0).toLocaleString();
    document.getElementById('cnt-pending').textContent  = (s.pending||0).toLocaleString();
    document.getElementById('cnt-errors').textContent   = (s.errors||0).toLocaleString();

    setSrcCard('kaggle', s);
    setSrcCard('gdrive', s);
    renderActivity(s.activity||[]);
    await loadFiles(currentTab);
    renderFileList();

    if(s.phase==='error'){
      document.getElementById('prog-text').textContent = 'Error: '+(s.error_msg||'');
    }
  }catch(_){}
  setTimeout(poll, 1000);
}
poll();
</script>
</body>
</html>
"""

# ---------- Health check -----------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"ok": True, "phase": state["phase"]}), 200

# ---------- Entry point ------------------------------------------------------
# Worker starts at module level so gunicorn (which never hits __main__) also
# gets the background thread.

_worker_thread = threading.Thread(target=worker, name="upload-worker", daemon=True)
_worker_thread.start()

if __name__ == "__main__":
    log.info("Dashboard (dev) -> http://0.0.0.0:%d", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)
