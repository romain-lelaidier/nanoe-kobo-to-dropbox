#!/usr/bin/env python3
"""
KoboToolbox -> Dropbox backup (robust version)

Per project it saves:
  asset.json            full asset detail (form content, settings, versions)
  form.xls / form.xml   XLSForm + XForm definitions (from the asset's `downloads`)
  submissions.json      all submissions (paginated)
  submissions.xlsx      Kobo's own XLSX export (via the exports API)
  attachments_partNNN.zip   media, grouped per submission, split in parts
  manifest.json         counts, sizes, sha256, failures

Design choices:
  * everything is staged on DISK (never whole files in RAM)
  * HTTP retries with backoff for Kobo; retries + size verification for Dropbox
  * one failing project / step never stops the others; exit code 1 if anything failed
  * archived projects are included (they still hold data)

Env vars (.env):
  KOBO_TOKEN, KOBO_SERVER (default https://eu.kobotoolbox.org)
    KOBO_TMP_DIR (default ~/.cache/kobo-to-dropbox)
  Dropbox, either:
    DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET   (recommended, no expiry)
    or DROPBOX_TOKEN                                               (short-lived tokens expire!)

pip install requests dropbox python-dotenv tqdm
"""
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import dropbox
import requests
from dotenv import load_dotenv
from dropbox.files import CommitInfo, UploadSessionCursor, WriteMode
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

load_dotenv()

KOBO_TOKEN = os.getenv("KOBO_TOKEN")
KOBO_SERVER = os.getenv("KOBO_SERVER", "https://eu.kobotoolbox.org").rstrip("/")
DROPBOX_ROOT = os.getenv("DROPBOX_ROOT")
IGNORE_UNTIL = os.getenv("IGNORE_UNTIL")
assert(DROPBOX_ROOT is not None)
TMP_ROOT = Path(os.getenv("KOBO_TMP_DIR", "~/.cache/kobo-to-dropbox")).expanduser()

INCLUDE_MEDIA = True
INCLUDE_XLSX_EXPORT = True
MAX_STORAGE_BYTES = 1 * 1024**3   # maximum staged files per project
MANIFEST_RESERVE_BYTES = 4 * 1024**2
MAX_ZIP_BYTES = 1 * 1024**3       # start a new attachments zip part after ~1 GB
DBX_CHUNK = 64 * 1024 * 1024       # Dropbox upload chunk (max allowed is 150 MB)
ATTACHMENT_ATTEMPTS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("kobo-backup")

# ----------------------------------------------------------------- Kobo HTTP
session = requests.Session()
session.headers["Authorization"] = f"Token {KOBO_TOKEN}"
_retry = Retry(
    total=6, connect=6, read=6, backoff_factor=2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    respect_retry_after_header=True,
)
session.mount("https://", HTTPAdapter(max_retries=_retry, pool_maxsize=10))
TIMEOUT = (15, 120)  # connect, read


def kget(url, **kw):
    kw.setdefault("timeout", TIMEOUT)
    return session.get(url, **kw)


def paginate(url, params=None):
    while url:
        r = kget(url, params=params)
        r.raise_for_status()
        data = r.json()
        yield from data["results"]
        url, params = data.get("next"), None


def slugify(name):
    s = re.sub(r"[^\w\-. ()]", "", name, flags=re.UNICODE).strip().replace(" ", "_")
    return s[:80] or "unnamed"


def temporary_directory(prefix=None):
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=TMP_ROOT)


def list_projects():
    params = {"q": "asset_type:survey", "limit": 100}
    for a in paginate(f"{KOBO_SERVER}/api/v2/assets/", params):
        if a.get("has_deployment") or a.get("deployment__active") is not None:
            yield a


class StorageBudget:
    def __init__(self, limit=MAX_STORAGE_BYTES, reserve=MANIFEST_RESERVE_BYTES):
        self.limit, self.reserve, self.used = limit, reserve, 0

    def claim(self, size, final=False):
        ceiling = self.limit if final else self.limit - self.reserve
        if size < 0 or self.used + size > ceiling:
            raise StorageLimitError(f"staging limit exceeded ({self.limit} bytes)")
        self.used += size

    def release(self, size):
        self.used = max(0, self.used - size)


class StorageLimitError(OSError):
    pass


def write_json(dest: Path, data, budget: StorageBudget, final=False):
    reserved = 0
    encoder = json.JSONEncoder(indent=2, ensure_ascii=False)
    try:
        with open(dest, "wb") as f:
            for part in encoder.iterencode(data):
                chunk = part.encode("utf-8")
                budget.claim(len(chunk), final=final)
                f.write(chunk)
                reserved += len(chunk)
    except Exception:
        budget.release(reserved)
        dest.unlink(missing_ok=True)
        raise


def stream_download(url, dest: Path, budget=None):
    """Stream to disk, verify length, return (size, sha256)."""
    h = hashlib.sha256()
    n = 0
    try:
        with kget(url, stream=True) as r:
            r.raise_for_status()
            expected = r.headers.get("Content-Length")
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if budget:
                        budget.claim(len(chunk))
                    f.write(chunk)
                    h.update(chunk)
                    n += len(chunk)
    except Exception:
        if budget:
            budget.release(n)
        dest.unlink(missing_ok=True)
        raise
    if expected and r.headers.get("Content-Encoding") is None and n != int(expected):
        if budget:
            budget.release(n)
        dest.unlink(missing_ok=True)
        raise IOError(f"truncated download: got {n} of {expected} bytes")
    return n, h.hexdigest()


def sha256_file(path: Path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


# ------------------------------------------------------------------- Dropbox
def make_dropbox():
    kw = dict(timeout=300, max_retries_on_error=10, max_retries_on_rate_limit=10)
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    if refresh_token:
        app_key, app_secret = os.getenv("DROPBOX_APP_KEY"), os.getenv("DROPBOX_APP_SECRET")
        if not app_key or not app_secret:
            raise RuntimeError(
                "DROPBOX_REFRESH_TOKEN requires DROPBOX_APP_KEY and DROPBOX_APP_SECRET"
            )
        return dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
            **kw,
        )
    access_token = os.getenv("DROPBOX_TOKEN")
    if not access_token:
        raise RuntimeError(
            "Configure DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, and DROPBOX_APP_SECRET"
        )
    log.warning("Dropbox access-token-only mode cannot refresh an expired token")
    return dropbox.Dropbox(oauth2_access_token=access_token, **kw)


def _upload_once(dbx, local: Path, remote: str):
    size = local.stat().st_size
    mode = WriteMode.overwrite
    with open(local, "rb") as f:
        if size <= DBX_CHUNK:
            dbx.files_upload(f.read(), remote, mode=mode)
            return
        sid = dbx.files_upload_session_start(f.read(DBX_CHUNK)).session_id
        cursor = UploadSessionCursor(session_id=sid, offset=f.tell())
        commit = CommitInfo(path=remote, mode=mode)
        while size - f.tell() > DBX_CHUNK:
            dbx.files_upload_session_append_v2(f.read(DBX_CHUNK), cursor)
            cursor.offset = f.tell()
        # always finish with the (non-empty, <= chunk) remainder
        dbx.files_upload_session_finish(f.read(DBX_CHUNK), cursor, commit)


def upload(dbx, local: Path, remote: str, attempts=10):
    """Upload from disk, then verify the size on Dropbox."""
    size = local.stat().st_size
    for i in range(1, attempts + 1):
        try:
            _upload_once(dbx, local, remote)
            remote_size = dbx.files_get_metadata(remote).size
            if remote_size != size:
                raise IOError(f"size mismatch on Dropbox ({remote_size} != {size})")
            return size
        except Exception as e:
            log.warning("upload %s failed (attempt %d/%d): %s", remote, i, attempts, e)
            if i == attempts:
                raise
            time.sleep(5 * i)


def _upload_stream_once(dbx, url, remote):
    digest = hashlib.sha256()
    size = 0
    with kget(url, stream=True) as response:
        response.raise_for_status()
        expected = response.headers.get("Content-Length")
        chunks = (chunk for chunk in response.iter_content(DBX_CHUNK) if chunk)
        first = next(chunks, b"")
        if not first:
            dbx.files_upload(b"", remote, mode=WriteMode.overwrite)
        else:
            digest.update(first)
            size += len(first)
            second = next(chunks, None)
            if second is None:
                dbx.files_upload(first, remote, mode=WriteMode.overwrite)
            else:
                start = dbx.files_upload_session_start(first)
                cursor = UploadSessionCursor(session_id=start.session_id, offset=len(first))
                pending = second
                for chunk in chunks:
                    dbx.files_upload_session_append_v2(pending, cursor)
                    cursor.offset += len(pending)
                    digest.update(pending)
                    size += len(pending)
                    pending = chunk
                dbx.files_upload_session_finish(
                    pending, cursor, CommitInfo(path=remote, mode=WriteMode.overwrite)
                )
                digest.update(pending)
                size += len(pending)
    if expected and response.headers.get("Content-Encoding") is None and size != int(expected):
        raise IOError(f"truncated media download: got {size} of {expected} bytes")
    remote_size = dbx.files_get_metadata(remote).size
    if remote_size != size:
        raise IOError(f"size mismatch on Dropbox ({remote_size} != {size})")
    return size, digest.hexdigest()


def upload_stream(dbx, url, remote, attempts=10):
    """Download from Kobo and upload to Dropbox without staging the media on disk."""
    for i in range(1, attempts + 1):
        try:
            return _upload_stream_once(dbx, url, remote)
        except Exception as e:
            log.warning("stream upload %s failed (attempt %d/%d): %s", remote, i, attempts, e)
            if i == attempts:
                raise
            time.sleep(5 * i)


# ---------------------------------------------------------------- Kobo steps
def save_definitions(asset_uid, tmp: Path, report, budget):
    r = kget(f"{KOBO_SERVER}/api/v2/assets/{asset_uid}/")
    r.raise_for_status()
    detail = r.json()
    write_json(tmp / "asset.json", detail, budget)
    report["files"].append("asset.json")
    for d in detail.get("downloads", []):
        fmt = d.get("format")
        if fmt in ("xls", "xml"):
            name = f"form.{fmt}"
            stream_download(d["url"], tmp / name, budget)
            report["files"].append(name)
    return detail


def fetch_submissions(asset_uid):
    url = f"{KOBO_SERVER}/api/v2/assets/{asset_uid}/data/"
    return list(paginate(url, {"format": "json", "limit": 1000}))


def export_xlsx(asset_uid, dest: Path, budget, max_wait=1800):
    body = {
        "type": "xls", "lang": "_default", "hierarchy_in_labels": False,
        "group_sep": "/", "multiple_select": "both", "fields_from_all_versions": True,
    }
    r = session.post(f"{KOBO_SERVER}/api/v2/assets/{asset_uid}/exports/", json=body, timeout=TIMEOUT)
    r.raise_for_status()
    url = r.json()["url"]
    deadline, delay = time.time() + max_wait, 3
    while time.time() < deadline:
        j = kget(url).json()
        if j.get("status") == "complete":
            return stream_download(j["result"], dest, budget)
        if j.get("status") == "error":
            raise RuntimeError(f"export failed: {j.get('messages')}")
        time.sleep(delay)
        delay = min(delay * 1.5, 30)
    raise TimeoutError("XLSX export did not finish in time")


class ZipParts:
    """Build, upload, and remove media ZIPs as each part is completed."""

    def __init__(self, folder: Path, budget: StorageBudget, dbx, remote_dir, report):
        self.folder, self.budget, self.dbx = folder, budget, dbx
        self.remote_dir, self.report = remote_dir, report
        self.n, self.zf, self.path, self.reserved = 0, None, None, 0

    def _open(self):
        self.n += 1
        path = self.folder / f"attachments_part{self.n:03d}.zip"
        # media is already compressed -> store, don't deflate
        archive = zipfile.ZipFile(path, "w", zipfile.ZIP_STORED, allowZip64=True)
        self.path, self.zf = path, archive

    def add(self, local: Path, arcname: str):
        size = local.stat().st_size
        overhead = 1024 + 2 * len(arcname.encode("utf-8"))
        if size + overhead > MAX_ZIP_BYTES:
            raise StorageLimitError(f"attachment exceeds ZIP part limit ({MAX_ZIP_BYTES} bytes)")
        if self.zf is not None and self.path.stat().st_size + size + overhead > MAX_ZIP_BYTES:
            self.close()
        self.budget.claim(size + overhead)
        self.reserved += size + overhead
        if self.zf is None:
            try:
                self._open()
            except Exception:
                self.budget.release(size + overhead)
                self.reserved -= size + overhead
                raise
        self.zf.write(local, arcname)

    def close(self):
        if self.path is None:
            return
        if self.zf:
            self.zf.close()
            self.zf = None
        size = self.path.stat().st_size
        digest = sha256_file(self.path)
        remote = f"{self.remote_dir}/{self.path.name}"
        upload(self.dbx, self.path, remote)
        self.path.unlink()
        self.budget.release(self.reserved)
        self.report.setdefault("media_archives", []).append(
            {"name": self.path.name, "bytes": size, "sha256": digest}
        )
        self.path, self.reserved = None, 0


def backup_media(submissions, tmp: Path, report, budget, dbx, remote_dir):
    zips = ZipParts(tmp, budget, dbx, remote_dir, report)
    scratch = tmp / "_dl"
    scratch.mkdir()
    seen, ok, failed = set(), 0, []
    atts = [(s, a) for s in submissions for a in s.get("_attachments", []) if not a.get("is_deleted")]
    report["attachments_listed"] = len(atts)
    for sub, att in tqdm(atts, desc="  media", unit="file", leave=False):
        url = att.get("download_url") or att.get("url")
        base = (att.get("filename") or "").rsplit("/", 1)[-1] or f"attachment_{att.get('id')}"
        arc = f"{sub.get('_id')}/{base}"
        if arc in seen:                                  # same name twice in one submission
            arc = f"{sub.get('_id')}/{att.get('id')}_{base}"
        seen.add(arc)
        safe_arc = "/".join(slugify(part) for part in arc.split("/"))
        direct_remote = f"{remote_dir}/attachments/{safe_arc}"
        tmpf = scratch / "current"
        size = 0
        for i in range(1, ATTACHMENT_ATTEMPTS + 1):
            try:
                if not url:
                    raise ValueError("no download url")
                size, _ = stream_download(url, tmpf, budget)
            except StorageLimitError:
                if zips.path is not None:
                    zips.close()
                    continue
                zips.close()
                try:
                    streamed_size, digest = upload_stream(dbx, url, direct_remote)
                except Exception as upload_error:
                    failed.append({"submission": sub.get("_id"), "file": base, "error": str(upload_error)})
                else:
                    report.setdefault("media_files", []).append(
                        {"path": direct_remote, "bytes": streamed_size, "sha256": digest}
                    )
                    ok += 1
                size = 0
                break
            except Exception as e:
                if i == ATTACHMENT_ATTEMPTS:
                    failed.append({"submission": sub.get("_id"), "file": base, "error": str(e)})
                else:
                    time.sleep(2 * i)
                continue

            try:
                zips.add(tmpf, arc)
            except StorageLimitError:
                if zips.path is not None:
                    zips.close()
                    try:
                        zips.add(tmpf, arc)
                    except StorageLimitError:
                        pass
                    else:
                        ok += 1
                        break
                digest = sha256_file(tmpf)
                try:
                    upload(dbx, tmpf, direct_remote)
                except Exception as upload_error:
                    failed.append({"submission": sub.get("_id"), "file": base, "error": str(upload_error)})
                    staged = tmp / f"attachment_{slugify(str(sub.get('_id')))}_{slugify(str(att.get('id')))}_{slugify(base)}"
                    tmpf.replace(staged)
                    size = 0
                    break
                report.setdefault("media_files", []).append(
                    {"path": direct_remote, "bytes": size, "sha256": digest}
                )
                tmpf.unlink(missing_ok=True)
                budget.release(size)
                size = 0
                ok += 1
                break
            except Exception:
                budget.release(size)
                tmpf.unlink(missing_ok=True)
                raise
            ok += 1
            break
        tmpf.unlink(missing_ok=True)
        if size:
            budget.release(size)
    report["attachments_ok"], report["attachments_failed"] = ok, failed
    zips.close()
    return report.get("media_archives", [])


# ---------------------------------------------------------------------- main
def backup_project(dbx, asset, root, report):
    uid = asset["uid"]
    remote_dir = f"{root}/{slugify(asset.get('name') or uid)}__{uid}"
    report.update(uid=uid, name=asset.get("name"), remote_dir=remote_dir, files=[], errors=[])

    def step(label, fn):
        try:
            return fn()
        except Exception as e:
            log.error("  ! %s failed: %s", label, e)
            report["errors"].append(f"{label}: {e}")

    with temporary_directory(prefix=f"kobo_{uid}_") as td:
        tmp = Path(td)
        log.info("path : %s", str(tmp))
        budget = StorageBudget()

        step("definitions", lambda: save_definitions(uid, tmp, report, budget))

        subs = step("submissions", lambda: fetch_submissions(uid))
        if subs is not None:
            step("submissions JSON", lambda: write_json(tmp / "submissions.json", subs, budget))
            report["submissions_fetched"] = len(subs)
            expected = asset.get("deployment__submission_count")
            report["submissions_expected"] = expected
            if expected is not None and expected != len(subs):
                log.warning("  submission count differs: listed %s, fetched %s", expected, len(subs))
            log.info("  %d submissions", len(subs))

        if INCLUDE_XLSX_EXPORT and (subs or subs is None):
            step("xlsx export", lambda: export_xlsx(uid, tmp / "submissions.xlsx", budget))

        if INCLUDE_MEDIA and subs:
            step("media", lambda: backup_media(subs, tmp, report, budget, dbx, remote_dir))
            if report.get("attachments_failed"):
                report["errors"].append(f"{len(report['attachments_failed'])} attachments failed")

        # manifest is written last so it describes what is really in the folder
        files = sorted(p for p in tmp.iterdir() if p.is_file())
        report["files"] = [
            {"name": p.name, "bytes": p.stat().st_size, "sha256": sha256_file(p)} for p in files
        ]
        step("manifest", lambda: write_json(tmp / "manifest.json", report, budget, final=True))

        for p in sorted(p for p in tmp.iterdir() if p.is_file()):
            step(f"upload {p.name}", lambda p=p: upload(dbx, p, f"{remote_dir}/{p.name}"))


def main():
    if not KOBO_TOKEN:
        sys.exit("KOBO_TOKEN missing")
    dbx = make_dropbox()
    dbx.users_get_current_account()  # fail fast on bad Dropbox credentials

    projects = list(list_projects())  # fail fast on bad Kobo credentials
    log.info("%d projects found", len(projects))

    run = {"started": datetime.now(timezone.utc).isoformat(), "projects": []}
    started = False
    for i, asset in enumerate(projects):
        if IGNORE_UNTIL is None or IGNORE_UNTIL == asset["uid"]:
            started = True
        if not started:
            log.info("(%i/%i) Ignoring: %s (%s)", i+1, len(projects), asset.get("name"), asset["uid"])
            continue
        log.info("(%i/%i) Processing: %s (%s)", i+1, len(projects), asset.get("name"), asset["uid"])
        report = {}
        try:
            backup_project(dbx, asset, DROPBOX_ROOT, report)
        except Exception as e:  # never let one project kill the run
            log.exception("project crashed")
            report.setdefault("errors", []).append(f"crash: {e}")
        run["projects"].append(report)

    run["finished"] = datetime.now(timezone.utc).isoformat()
    failed = [p for p in run["projects"] if p.get("errors")]
    run["projects_with_errors"] = len(failed)
    with temporary_directory(prefix="kobo_summary_") as td:
        summary = Path(td) / "run_summary.json"
        summary.write_text(json.dumps(run, indent=2, ensure_ascii=False), "utf-8")
        upload(dbx, summary, f"{DROPBOX_ROOT}/run_summary_{datetime.now(timezone.utc).isoformat()}.json")

    log.info("Done. %d/%d projects with errors. Output: %s", len(failed), len(projects), DROPBOX_ROOT)
    for p in failed:
        log.error("  %s: %s", p.get("name"), "; ".join(p["errors"]))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
