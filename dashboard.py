"""
Kaggle -> S3 sync worker with live web dashboard.
Open the Railway public URL to watch progress in real time.
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
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from flask import Flask, jsonify, Response

load_dotenv()

# ---------- Config -----------------------------------------------------------

KAGGLE_DATASET  = "ammarshafiq/supreme-court-of-pakistan-judgments-dataset"
S3_BUCKET       = os.getenv("S3_BUCKET_NAME", "assancase-documents-adminpenta")
S3_PREFIX       = os.getenv("S3_PREFIX", "supreme-court-pakistan/")
AWS_REGION      = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
MANIFEST_KEY    = f"{S3_PREFIX}manifest.json"
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", "1"))
PORT            = int(os.getenv("PORT", "8080"))
WORK_DIR        = Path(os.getenv("WORK_DIR", tempfile.gettempdir())) / "kaggle_s3"
MAX_ACTIVITY    = 200

# ---------- Shared state (written by worker, read by Flask) ------------------

state = {
    "phase":        "starting",   # starting|downloading|extracting|uploading|complete|error
    "total":        0,
    "uploaded":     0,
    "skipped":      0,
    "errors":       0,
    "pending":      0,
    "tick":         0,
    "elapsed_s":    0,
    "start_time":   None,
    "error_msg":    "",
    "activity":     [],            # [{type, name, size_kb, ts}]
    "uploaded_files":  [],         # full list
    "skipped_files":   [],         # full list
    "error_files":     [],         # full list
}
state_lock = threading.Lock()

log = logging.getLogger("worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

# ---------- Helpers ----------------------------------------------------------

def md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


def push_activity(kind: str, name: str, size_kb: float = 0) -> None:
    entry = {
        "type": kind,
        "name": name,
        "size_kb": round(size_kb, 1),
        "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
    }
    with state_lock:
        state["activity"].insert(0, entry)
        if len(state["activity"]) > MAX_ACTIVITY:
            state["activity"].pop()
        if kind == "uploaded":
            state["uploaded_files"].append(name)
        elif kind == "skipped":
            state["skipped_files"].append(name)
        elif kind == "error":
            state["error_files"].append(name)


def set_phase(phase: str, **kwargs) -> None:
    with state_lock:
        state["phase"] = phase
        state.update(kwargs)


def update_counts(**kwargs) -> None:
    with state_lock:
        state.update(kwargs)


def fmt_size(path: Path) -> float:
    try:
        return path.stat().st_size / 1024
    except Exception:
        return 0.0


def load_manifest(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise


def save_manifest(s3, manifest: dict) -> None:
    body = json.dumps(manifest, indent=2).encode()
    s3.put_object(Bucket=S3_BUCKET, Key=MANIFEST_KEY, Body=body,
                  ContentType="application/json")


def verify_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            raise PermissionError(f"Access denied to bucket '{S3_BUCKET}'.")
        raise


def setup_kaggle() -> None:
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


def download_dataset() -> Path:
    from kaggle import KaggleApi
    api = KaggleApi()
    api.authenticate()

    zip_dir = WORK_DIR / "raw"
    zip_dir.mkdir(parents=True, exist_ok=True)

    set_phase("downloading")
    log.info("Downloading Kaggle dataset ...")
    api.dataset_download_files(KAGGLE_DATASET, path=str(zip_dir), unzip=False, quiet=False)

    zips = list(zip_dir.glob("*.zip"))
    if not zips:
        raise FileNotFoundError("No zip found after Kaggle download.")

    set_phase("extracting")
    extract_dir = WORK_DIR / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)
    for z in zips:
        log.info("Extracting %s ...", z.name)
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extract_dir)

    return extract_dir


def build_index(src: Path) -> dict[str, Path]:
    return {
        S3_PREFIX + f.relative_to(src).as_posix(): f
        for f in src.rglob("*") if f.is_file()
    }


# ---------- Upload worker ----------------------------------------------------

def worker() -> None:
    start = datetime.now(timezone.utc)
    set_phase("starting", start_time=start.isoformat())

    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        verify_bucket(s3)

        setup_kaggle()

        extract_dir = WORK_DIR / "extracted"
        if not extract_dir.exists() or not any(extract_dir.rglob("*")):
            extract_dir = download_dataset()
        else:
            log.info("Using cached extraction at %s", extract_dir)

        file_index = build_index(extract_dir)
        total = len(file_index)
        set_phase("uploading", total=total)
        log.info("Total files: %d  — polling every %ds", total, POLL_INTERVAL)

        tick = 0
        while True:
            tick += 1
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()

            manifest = load_manifest(s3)

            uploaded_keys = set()
            skipped_keys  = set()
            pending       = {}

            for key, path in file_index.items():
                checksum = md5(path)
                if manifest.get(key) == checksum:
                    skipped_keys.add(key)
                else:
                    pending[key] = (path, checksum)

            already_uploaded = len(uploaded_keys)  # from previous ticks — tracked in manifest
            n_skipped = len(skipped_keys)
            n_pending = len(pending)
            n_uploaded = total - n_pending - n_skipped  # already done in manifest

            # Sync skipped list into state (first tick only, or when it grows)
            with state_lock:
                known_skipped = set(state["skipped_files"])
            for key in skipped_keys:
                name = key.replace(S3_PREFIX, "")
                if name not in known_skipped:
                    push_activity("skipped", name, fmt_size(file_index[key]))

            update_counts(
                tick=tick,
                elapsed_s=round(elapsed),
                uploaded=n_uploaded,
                skipped=n_skipped,
                pending=n_pending,
            )

            if not pending:
                save_manifest(s3, manifest)
                elapsed_final = (datetime.now(timezone.utc) - start).total_seconds()
                set_phase("complete", elapsed_s=round(elapsed_final),
                          uploaded=n_uploaded, skipped=n_skipped, pending=0, errors=0)
                log.info("All %d files confirmed in S3. Done in %.0fs.", total, elapsed_final)
                break

            errors = 0
            for s3_key, (local, checksum) in pending.items():
                name = s3_key.replace(S3_PREFIX, "")
                try:
                    log.info("Uploading -> %s", s3_key)
                    size_kb = fmt_size(local)
                    s3.upload_file(
                        str(local), S3_BUCKET, s3_key,
                        ExtraArgs={"ContentType": _content_type(local)},
                    )
                    manifest[s3_key] = checksum
                    push_activity("uploaded", name, size_kb)
                except ClientError as e:
                    log.error("Error: %s — %s", s3_key, e)
                    push_activity("error", name)
                    errors += 1

            with state_lock:
                state["errors"] = errors

            save_manifest(s3, manifest)
            time.sleep(POLL_INTERVAL)

    except Exception as exc:
        log.exception("Worker crashed: %s", exc)
        set_phase("error", error_msg=str(exc))


def _content_type(path: Path) -> str:
    return {
        ".csv":  "text/csv",
        ".json": "application/json",
        ".pdf":  "application/pdf",
        ".txt":  "text/plain",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(path.suffix.lower(), "application/octet-stream")


# ---------- Flask dashboard --------------------------------------------------

app = Flask(__name__)


@app.route("/status")
def status_api():
    with state_lock:
        return jsonify(dict(state))


@app.route("/files/<kind>")
def file_list(kind):
    with state_lock:
        data = {
            "uploaded": state["uploaded_files"],
            "skipped":  state["skipped_files"],
            "errors":   state["error_files"],
        }.get(kind, [])
    return jsonify(data)


@app.route("/")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>S3 Upload Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body { background: #0f172a; }
  .card { background: #1e293b; border: 1px solid #334155; }
  .badge-uploaded { background:#166534; color:#bbf7d0; }
  .badge-skipped  { background:#1e3a5f; color:#93c5fd; }
  .badge-error    { background:#7f1d1d; color:#fca5a5; }
  .badge-pending  { background:#44403c; color:#d6d3d1; }
  .prog-bar { transition: width 0.5s ease; }
  ::-webkit-scrollbar { width:6px; }
  ::-webkit-scrollbar-track { background:#0f172a; }
  ::-webkit-scrollbar-thumb { background:#475569; border-radius:3px; }
</style>
</head>
<body class="text-slate-200 min-h-screen p-6 font-mono text-sm">

<!-- Header -->
<div class="max-w-6xl mx-auto">
  <div class="flex items-center justify-between mb-6">
    <div>
      <h1 class="text-xl font-bold text-white">Supreme Court Judgments &rarr; S3</h1>
      <p class="text-slate-400 text-xs mt-1">s3://assancase-documents-adminpenta/supreme-court-pakistan/</p>
    </div>
    <div class="flex items-center gap-2">
      <span id="phase-dot" class="w-2.5 h-2.5 rounded-full bg-yellow-400 animate-pulse"></span>
      <span id="phase-label" class="text-slate-300 uppercase text-xs tracking-widest">starting</span>
    </div>
  </div>

  <!-- Progress bar -->
  <div class="card rounded-xl p-5 mb-4">
    <div class="flex justify-between text-xs text-slate-400 mb-2">
      <span id="prog-text">Initialising...</span>
      <span id="prog-pct">0%</span>
    </div>
    <div class="w-full bg-slate-700 rounded-full h-3 overflow-hidden">
      <div id="prog-bar" class="prog-bar h-3 rounded-full bg-gradient-to-r from-emerald-500 to-teal-400" style="width:0%"></div>
    </div>
    <div class="flex justify-between text-xs text-slate-500 mt-2">
      <span id="elapsed">Elapsed: --</span>
      <span id="tick-info">Tick: 0 &bull; Poll: 1s</span>
    </div>
  </div>

  <!-- Stats row -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
    <div class="card rounded-xl p-4">
      <div class="text-2xl font-bold text-emerald-400" id="cnt-uploaded">0</div>
      <div class="text-xs text-slate-400 mt-1">Uploaded</div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-2xl font-bold text-blue-400" id="cnt-skipped">0</div>
      <div class="text-xs text-slate-400 mt-1">Skipped (already in S3)</div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-2xl font-bold text-slate-300" id="cnt-pending">0</div>
      <div class="text-xs text-slate-400 mt-1">Pending</div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-2xl font-bold text-red-400" id="cnt-errors">0</div>
      <div class="text-xs text-slate-400 mt-1">Errors</div>
    </div>
  </div>

  <!-- Two-column: activity + file lists -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">

    <!-- Live activity feed -->
    <div class="card rounded-xl p-4">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-3">Live Activity</h2>
      <div id="activity-feed" class="space-y-1 max-h-80 overflow-y-auto pr-1"></div>
    </div>

    <!-- File lists tabs -->
    <div class="card rounded-xl p-4">
      <div class="flex gap-2 mb-3">
        <button onclick="showTab('uploaded')" id="tab-uploaded"
          class="tab-btn text-xs px-3 py-1 rounded-full badge-uploaded">Uploaded</button>
        <button onclick="showTab('skipped')" id="tab-skipped"
          class="tab-btn text-xs px-3 py-1 rounded-full badge-skipped">Skipped</button>
        <button onclick="showTab('errors')" id="tab-errors"
          class="tab-btn text-xs px-3 py-1 rounded-full badge-error">Errors</button>
      </div>
      <div id="file-list" class="max-h-80 overflow-y-auto pr-1 space-y-1 text-xs text-slate-300"></div>
    </div>
  </div>
</div>

<script>
let currentTab = 'uploaded';
let fileCache  = { uploaded: [], skipped: [], errors: [] };

function fmtElapsed(s) {
  if (s < 60) return s + 's';
  const m = Math.floor(s / 60), sec = s % 60;
  return m + 'm ' + sec + 's';
}

function showTab(kind) {
  currentTab = kind;
  renderFileList();
}

function renderFileList() {
  const files = fileCache[currentTab] || [];
  const el = document.getElementById('file-list');
  if (!files.length) { el.innerHTML = '<div class="text-slate-500 italic">None yet.</div>'; return; }
  el.innerHTML = files.slice().reverse().map(f => {
    const short = f.split('/').pop();
    return `<div class="truncate text-slate-300 hover:text-white" title="${f}">${short}</div>`;
  }).join('');
}

async function loadFiles(kind) {
  try {
    const r = await fetch('/files/' + kind);
    fileCache[kind] = await r.json();
  } catch (_) {}
}

function renderActivity(items) {
  const feed = document.getElementById('activity-feed');
  const colors = {
    uploaded: 'badge-uploaded',
    skipped:  'badge-skipped',
    error:    'badge-error',
  };
  feed.innerHTML = items.map(e => {
    const short = e.name.split('/').pop();
    const badge = colors[e.type] || 'badge-pending';
    const size  = e.size_kb > 0 ? ` <span class="text-slate-500">${e.size_kb > 1024
      ? (e.size_kb/1024).toFixed(1)+'MB'
      : e.size_kb.toFixed(0)+'KB'}</span>` : '';
    return `<div class="flex items-center gap-2 py-0.5">
      <span class="text-slate-600 text-xs w-16 shrink-0">${e.ts}</span>
      <span class="text-xs px-1.5 py-0.5 rounded ${badge} uppercase shrink-0">${e.type}</span>
      <span class="truncate text-xs" title="${e.name}">${short}${size}</span>
    </div>`;
  }).join('');
}

const phaseConfig = {
  starting:    { dot:'bg-yellow-400 animate-pulse', label:'Starting' },
  downloading: { dot:'bg-sky-400 animate-pulse',    label:'Downloading' },
  extracting:  { dot:'bg-violet-400 animate-pulse', label:'Extracting' },
  uploading:   { dot:'bg-emerald-400 animate-pulse',label:'Uploading' },
  complete:    { dot:'bg-emerald-400',              label:'Complete' },
  error:       { dot:'bg-red-500',                  label:'Error' },
};

async function poll() {
  try {
    const r = await fetch('/status');
    const s = await r.json();

    // Phase
    const pc = phaseConfig[s.phase] || phaseConfig.starting;
    document.getElementById('phase-dot').className   = `w-2.5 h-2.5 rounded-full ${pc.dot}`;
    document.getElementById('phase-label').textContent = pc.label;

    // Progress
    const total = s.total || 1;
    const done  = s.uploaded + s.skipped;
    const pct   = total ? Math.round((done / total) * 100) : 0;
    document.getElementById('prog-bar').style.width  = pct + '%';
    document.getElementById('prog-pct').textContent  = pct + '%';
    document.getElementById('prog-text').textContent =
      `${done.toLocaleString()} / ${total.toLocaleString()} files`;

    document.getElementById('elapsed').textContent   = 'Elapsed: ' + fmtElapsed(s.elapsed_s || 0);
    document.getElementById('tick-info').textContent = `Tick: ${s.tick} • Poll: 1s`;

    // Counters
    document.getElementById('cnt-uploaded').textContent = (s.uploaded||0).toLocaleString();
    document.getElementById('cnt-skipped').textContent  = (s.skipped||0).toLocaleString();
    document.getElementById('cnt-pending').textContent  = (s.pending||0).toLocaleString();
    document.getElementById('cnt-errors').textContent   = (s.errors||0).toLocaleString();

    // Activity
    renderActivity(s.activity || []);

    // File lists (refresh only active tab)
    await loadFiles(currentTab);
    renderFileList();

    if (s.phase === 'error') {
      document.getElementById('prog-text').textContent = 'Error: ' + (s.error_msg || '');
    }
  } catch (_) {}

  setTimeout(poll, 1000);
}

poll();
</script>
</body>
</html>
"""


# ---------- Entry point ------------------------------------------------------

if __name__ == "__main__":
    t = threading.Thread(target=worker, name="upload-worker", daemon=True)
    t.start()

    log.info("Dashboard running on http://0.0.0.0:%d", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)
