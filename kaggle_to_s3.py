"""
Automation agent: Kaggle Supreme Court of Pakistan dataset -> S3
Dataset: ammarshafiq/supreme-court-of-pakistan-judgments-dataset
Bucket:  assancase-documents-adminpenta (us-east-1)
"""

import os
import sys
import json
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

# ---------- Helpers ----------------------------------------------------------

def md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


def load_manifest(s3) -> dict:
    """Fetch the manifest of already-uploaded files from S3 (key -> md5)."""
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
    log.info("Manifest saved to s3://%s/%s", S3_BUCKET, MANIFEST_KEY)


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
    ext = path.suffix.lower()
    return {
        ".csv":  "text/csv",
        ".json": "application/json",
        ".pdf":  "application/pdf",
        ".txt":  "text/plain",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".zip":  "application/zip",
    }.get(ext, "application/octet-stream")


# ---------- Kaggle credentials -----------------------------------------------

def _setup_kaggle_credentials() -> None:
    """
    Configure Kaggle auth from environment variables before importing kaggle SDK.
      KGAT_... token  -> KAGGLE_API_TOKEN env var + ~/.kaggle/access_token file
      legacy key      -> ~/.kaggle/kaggle.json  (username + key UUID)
    """
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)

    token    = os.getenv("KAGGLE_TOKEN")    # new KGAT_... bearer token
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")

    if token:
        # Set the env var the SDK reads at import time
        os.environ["KAGGLE_API_TOKEN"] = token
        # Also write to the file-based fallback the SDK checks
        access_token_file = kaggle_dir / "access_token"
        access_token_file.write_text(token, encoding="utf-8")
        log.info("Kaggle KGAT token written to %s", access_token_file)
    elif username and key:
        kaggle_cfg = kaggle_dir / "kaggle.json"
        kaggle_cfg.write_text(
            json.dumps({"username": username, "key": key}), encoding="utf-8"
        )
        log.info("Kaggle legacy credentials written to %s", kaggle_cfg)
    else:
        raise EnvironmentError(
            "Set KAGGLE_TOKEN (KGAT_... format) or both KAGGLE_USERNAME + KAGGLE_KEY in .env"
        )


# ---------- Kaggle download --------------------------------------------------

def download_kaggle_dataset(dest_dir: Path) -> Path:
    """Download dataset zip via kaggle SDK and return extracted directory."""
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
        raise FileNotFoundError("No zip file found after Kaggle download.")

    extract_dir = dest_dir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)
    for z in zips:
        log.info("Extracting %s ...", z.name)
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extract_dir)

    return extract_dir


# ---------- S3 upload --------------------------------------------------------

def upload_directory(s3, src: Path, manifest: dict) -> dict:
    """
    Walk src recursively, skip already-uploaded (same md5), upload the rest.
    Returns updated manifest.
    """
    files = sorted(f for f in src.rglob("*") if f.is_file())
    log.info("Found %d files to consider for upload.", len(files))

    new_count = skip_count = err_count = 0
    for f in files:
        rel      = f.relative_to(src)
        s3_key   = S3_PREFIX + rel.as_posix()
        checksum = md5(f)

        if manifest.get(s3_key) == checksum:
            log.debug("Skip (unchanged): %s", s3_key)
            skip_count += 1
            continue

        try:
            log.info("Uploading -> s3://%s/%s", S3_BUCKET, s3_key)
            upload_file(s3, f, s3_key)
            manifest[s3_key] = checksum
            new_count += 1
        except ClientError as e:
            log.error("Failed to upload %s: %s", s3_key, e)
            err_count += 1

    log.info("Upload summary: %d new, %d skipped, %d errors.",
             new_count, skip_count, err_count)
    return manifest


# ---------- Verify bucket ----------------------------------------------------

def verify_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        log.info("Bucket verified: s3://%s", S3_BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            raise PermissionError(
                f"Access denied to bucket '{S3_BUCKET}'. Check IAM permissions."
            )
        if code in ("404", "NoSuchBucket"):
            raise FileNotFoundError(
                f"Bucket '{S3_BUCKET}' not found in {AWS_REGION}."
            )
        raise


# ---------- CLI --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kaggle -> S3 automation agent")
    p.add_argument(
        "--work-dir", type=Path,
        default=Path(tempfile.gettempdir()) / "kaggle_s3",
        help="Local working directory for downloads (default: system temp)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Download and extract but do NOT upload to S3.",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-upload all files even if md5 matches manifest.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    start = datetime.now(timezone.utc)
    log.info("=== Kaggle -> S3 agent started at %s ===", start.isoformat())

    # AWS client
    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        if not args.dry_run:
            verify_bucket(s3)
    except NoCredentialsError:
        log.error("No AWS credentials. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env")
        sys.exit(1)

    # Download
    extract_dir = download_kaggle_dataset(args.work_dir)
    log.info("Dataset extracted to: %s", extract_dir)

    if args.dry_run:
        log.info("--dry-run: skipping S3 upload.")
        return

    # Upload
    manifest = {} if args.force else load_manifest(s3)
    manifest = upload_directory(s3, extract_dir, manifest)
    save_manifest(s3, manifest)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    log.info("=== Done in %.1fs ===", elapsed)


if __name__ == "__main__":
    main()
