import glob
import logging
import os
import shutil
import time

logger = logging.getLogger(__name__)


def _iter_process_cmdlines():
    """
    Yields (pid, argv_list) for processes we can read from /proc.
    """
    for cmdline_path in glob.glob("/proc/[0-9]*/cmdline"):
        pid = cmdline_path.split("/")[2]
        try:
            with open(cmdline_path, "rb") as f:
                raw = f.read()
            if not raw:
                continue
            argv = [part.decode("utf-8", errors="ignore") for part in raw.split(b"\0") if part]
            if argv:
                yield pid, argv
        except Exception:
            # Some processes may disappear or be unreadable; ignore.
            continue


def _collect_in_use_user_data_dirs():
    """
    Returns a set of user-data-dir paths currently used by any process on the host.
    We only care about args of the form:
      --user-data-dir=/path
      --user-data-dir /path
    """
    in_use = set()
    for _pid, argv in _iter_process_cmdlines():
        for idx, arg in enumerate(argv):
            if arg.startswith("--user-data-dir="):
                in_use.add(arg.split("=", 1)[1])
            elif arg == "--user-data-dir" and idx + 1 < len(argv):
                in_use.add(argv[idx + 1])
    return in_use


def cleanup_attendee_chrome_profiles(
    *,
    profiles_root: str = "/tmp",
    prefix: str = "attendee-chrome-profile-",
    max_age_seconds: int,
) -> dict:
    """
    Deletes old `attendee-chrome-profile-*` directories in /tmp that are not referenced
    by any currently running process via `--user-data-dir`.
    """
    now = time.time()
    pattern = os.path.join(profiles_root, f"{prefix}*")

    candidates = [p for p in glob.glob(pattern) if os.path.isdir(p)]
    in_use = _collect_in_use_user_data_dirs()

    deleted = []
    skipped_in_use = []
    skipped_too_new = []
    failed = []

    for path in candidates:
        # Safety: only touch expected directories directly under profiles_root.
        if os.path.dirname(path.rstrip("/")) != profiles_root.rstrip("/"):
            continue
        if not os.path.basename(path).startswith(prefix):
            continue

        try:
            age_seconds = now - os.path.getmtime(path)
        except Exception as e:
            failed.append((path, f"stat_failed:{e.__class__.__name__}"))
            continue

        if age_seconds < max_age_seconds:
            skipped_too_new.append(path)
            continue

        if path in in_use:
            skipped_in_use.append(path)
            continue

        try:
            shutil.rmtree(path, ignore_errors=False)
            deleted.append(path)
        except Exception as e:
            failed.append((path, f"delete_failed:{e.__class__.__name__}"))

    summary = {
        "candidates": len(candidates),
        "deleted": len(deleted),
        "skipped_in_use": len(skipped_in_use),
        "skipped_too_new": len(skipped_too_new),
        "failed": len(failed),
    }

    if summary["deleted"] or summary["failed"]:
        logger.info(
            "Chrome profile cleanup result: %s (max_age_seconds=%s, root=%s, prefix=%s)",
            summary,
            max_age_seconds,
            profiles_root,
            prefix,
        )

    return summary

