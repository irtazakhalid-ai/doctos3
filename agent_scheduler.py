"""
Optional scheduler wrapper — runs kaggle_to_s3.py on a cron-like interval.
Usage:
    python agent_scheduler.py --interval 3600          # run every hour
    python agent_scheduler.py --interval 86400 --once  # run once then exit
"""

import time
import logging
import argparse
import subprocess
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCHEDULER] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def run_agent(extra_args: list[str]) -> int:
    cmd = [sys.executable, "kaggle_to_s3.py"] + extra_args
    log.info("Launching: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    return result.returncode


def main() -> None:
    p = argparse.ArgumentParser(description="Scheduler for kaggle_to_s3 agent")
    p.add_argument("--interval", type=int, default=86400,
                   help="Seconds between runs (default: 86400 = 24h)")
    p.add_argument("--once", action="store_true",
                   help="Run once and exit (ignore --interval)")
    p.add_argument("--dry-run", action="store_true",
                   help="Pass --dry-run to the agent")
    p.add_argument("--force", action="store_true",
                   help="Pass --force to the agent")
    args = p.parse_args()

    extra = []
    if args.dry_run:
        extra.append("--dry-run")
    if args.force:
        extra.append("--force")

    if args.once:
        code = run_agent(extra)
        sys.exit(code)

    log.info("Scheduler started — running every %ds. Ctrl+C to stop.", args.interval)
    while True:
        t0 = datetime.now(timezone.utc)
        log.info("── Run at %s ──", t0.isoformat())
        code = run_agent(extra)
        log.info("Agent exited with code %d", code)
        log.info("Next run in %ds …", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
