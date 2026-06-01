"""
Kaggle -> S3 sync worker
- Downloads Supreme Court of Pakistan dataset from Kaggle
- Polls every second: uploads any file not yet in S3
- Exits with code 0 when all files are confirmed uploaded
"""

import os
import sys
import json
import time
import zipfile
import logging
import hashlib
import tempfile
import argparse
from pathlib import Path
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from tqdm import tqdm

# ---------- Logging ----------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
        ),
        logging.FileHandler("upload_run.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------- Config -----------------------------------------------------------

load_dotenv()

KAGGLE_DATASET = "ammarshafiq/supreme-court-of-pakistan-judgments-dataset"
S3_BUCKET      = os.getenv("S3_BUCKET_NAME", "assancase-documents-adminpenta")
S3_PREFIX      = os.getenv("S3_PREFIX", "supreme-court-pakistan/")
AWS_REGION     = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
MANIFEST_KEY   = f"{S3_PREFIX}manifest.json"
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL_SECONDS", "1"))

# ---------- Helpers ----------------------------------------------------------

def md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


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
    log.info("Manifest saved -> s3://%s/%s", S3_BUCKET, MANIFEST_KEY)


def upload_file(s3, local: Path, s3_key: str) -> None:
    size = local.stat().st_size
    with tqdm(total=size, unit="B", unit_scale=True,
              desc=local.name, leave=False) as bar:
        s3.upload_file(
            str(local), S3_BUCKET, s3_key,
            Callback=lambda n: bar.update(n),
            ExtraArgs={"ContentType": _content_type(local)},
        )


def _content_type(path: Path) -> str:
    return {
        ".csv":  "text/csv",
        ".json": "application/json",
        ".pdf":  "application/pdf",
        ".txt":  "text/plain",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".zip":  "application/zip",
    }.get(path.suffix.lower(), "application/octet-stream")


def verify_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        log.info("Bucket verified: s3://%s", S3_BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            raise PermissionError(f"Access denied to '{S3_BUCKET}'. Check IAM permissions.")
        if code in ("404", "NoSuchBucket"):
            raise FileNotFoundError(f"Bucket '{S3_BUCKET}' not found in {AWS_REGION}.")
        raise


# ---------- Kaggle -----------------------------------------------------------

def _setup_kaggle_credentials() -> None:
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)

    token    = os.getenv("KAGGLE_TOKEN")
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")

    if token:
        os.environ["KAGGLE_API_TOKEN"] = token
        (kaggle_dir / "access_token").write_text(token, encoding="utf-8")
        log.info("Kaggle KGAT token configured.")
    elif username and key:
        (kaggle_dir / "kaggle.json").write_text(
            json.dumps({"username": username, "key": key}), encoding="utf-8"
        )
        log.info("Kaggle legacy credentials configured.")
    else:
        raise EnvironmentError(
            "Set KAGGLE_TOKEN (KGAT_ format) or both KAGGLE_USERNAME + KAGGLE_KEY."
        )


def download_kaggle_dataset(dest_dir: Path) -> Path:
    _setup_kaggle_credentials()

    from kaggle import KaggleApi
    api = KaggleApi()
    api.authenticate()

    zip_dir = dest_dir / "raw"
    zip_dir.mkdir(parents=True, exist_ok=True)

    log.info("Downloading Kaggle dataset '%s' ...", KAGGLE_DATASET)
    api.dataset_download_files(KAGGLE_DATASET, path=str(zip_dir), unzip=False, quiet=False)

    zips = list(zip_dir.glob("*.zip"))
    if not zips:
        raise FileNotFoundError("No zip found after Kaggle download.")

    extract_dir = dest_dir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)
    for z in zips:
        log.info("Extracting %s ...", z.name)
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extract_dir)

    return extract_dir


# ---------- Upload loop ------------------------------------------------------

def build_file_index(src: Path) -> dict[str, Path]:
    """Return {s3_key: local_path} for every file under src."""
    return {
        S3_PREFIX + f.relative_to(src).as_posix(): f
        for f in src.rglob("*") if f.is_file()
    }


def upload_loop(s3, file_index: dict[str, Path]) -> None:
    """
    Poll every POLL_INTERVAL seconds.
    Each tick: load the manifest, find pending files, upload them.
    Exit when manifest confirms every file is uploaded.
    """
    total = len(file_index)
    log.info("Starting upload loop — %d files to sync, polling every %ds.",
             total, POLL_INTERVAL)

    while True:
        manifest = load_manifest(s3)

        # Build pending list (missing or md5-changed)
        pending = {
            key: path for key, path in file_index.items()
            if manifest.get(key) != md5(path)
        }

        done = total - len(pending)
        log.info("Progress: %d / %d uploaded. %d pending.",
                 done, total, len(pending))

        if not pending:
            log.info("All %d files confirmed in S3. Done.", total)
            save_manifest(s3, manifest)
            break

        # Upload every pending file this tick
        errors = 0
        for s3_key, local in pending.items():
            try:
                log.info("Uploading -> s3://%s/%s", S3_BUCKET, s3_key)
                upload_file(s3, local, s3_key)
                manifest[s3_key] = md5(local)
            except ClientError as e:
                log.error("Error uploading %s: %s", s3_key, e)
                errors += 1

        # Persist manifest after each tick so progress survives restarts
        save_manifest(s3, manifest)

        if errors:
            log.warning("%d file(s) failed this tick — will retry next tick.", errors)

        time.sleep(POLL_INTERVAL)


# ---------- CLI --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kaggle -> S3 sync worker")
    p.add_argument(
        "--work-dir", type=Path,
        default=Path(os.getenv("WORK_DIR", tempfile.gettempdir())) / "kaggle_s3",
        help="Local directory for downloads.",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Ignore existing manifest and re-upload everything.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    start = datetime.now(timezone.utc)
    log.info("=== Kaggle -> S3 worker started at %s ===", start.isoformat())

    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        verify_bucket(s3)
    except NoCredentialsError:
        log.error("No AWS credentials. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.")
        sys.exit(1)

    # Download dataset (idempotent — skips if already extracted)
    extract_dir = args.work_dir / "extracted"
    if not extract_dir.exists() or not any(extract_dir.rglob("*")):
        extract_dir = download_kaggle_dataset(args.work_dir)
    else:
        log.info("Using cached extraction at %s", extract_dir)

    # Build index of all local files
    file_index = build_file_index(extract_dir)
    log.info("Total files in dataset: %d", len(file_index))

    if args.force:
        log.info("--force: clearing manifest.")
        save_manifest(s3, {})

    # Run the polling upload loop — exits when everything is confirmed in S3
    upload_loop(s3, file_index)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    log.info("=== Worker finished in %.1fs. Exiting. ===", elapsed)


if __name__ == "__main__":
    main()
