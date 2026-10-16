"""
github_sync.py
--------------
Keeps the database saved in GitHub so a cloud-hosted dashboard doesn't lose
new games when it restarts.
 
  pull_db()  - before a check: download the newest database from GitHub, so
               the check starts from what is really saved there
  push_db()  - after a check that added games: upload the database again
 
Safety: GitHub only accepts an upload if the file it is replacing is the same
version we downloaded (the "sha"). If a teammate saved a newer database in the
meantime, the upload is refused and nothing is overwritten.
 
Needs these Streamlit secrets (see the setup notes):
    github_token      required - a fine-grained token with Contents: read & write
    github_repo       optional - default "CarlAlmer/Plus-Minus"
    github_branch     optional - default "main"
    github_data_dir   optional - default "." (the top of the repo). Set it to the folder
                      holding basketball.db in the repo, e.g. "data".
"""
 
import base64
import os
import tempfile
from typing import Dict, Optional, Tuple
 
import requests
 
API = "https://api.github.com"
SQLITE_MAGIC = b"SQLite format 3\x00"
 
 
class SyncError(Exception):
    """Something went wrong talking to GitHub; the message is safe to show."""
 
 
def config_from_secrets(secrets) -> Optional[Dict]:
    """Settings from st.secrets, or None when no token is configured."""
    def get(key, default=None):
        try:
            value = secrets[key]
        except Exception:
            return default
        return value or default
 
    token = get("github_token")
    if not token:
        return None
    return {
        "token": token,
        "repo": get("github_repo", "CarlAlmer/Plus-Minus"),
        "branch": get("github_branch", "main"),
        "data_dir": get("github_data_dir", "."),
    }
 
 
def repo_path_for(cfg: Dict, db_name: str) -> str:
    folder = cfg["data_dir"].strip().strip("/")
    if folder in (".", ""):
        return db_name
    return f"{folder}/{db_name}"
 
 
def _headers(cfg: Dict, raw: bool = False) -> Dict:
    return {
        "Authorization": f"Bearer {cfg['token']}",
        "Accept": "application/vnd.github.raw+json" if raw else "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "UWRF-PlusMinus",
    }
 
 
def _contents_url(cfg: Dict, path: str) -> str:
    return f"{API}/repos/{cfg['repo']}/contents/{path}"
 
 
def _explain(status: int, text: str) -> str:
    if status == 401:
        return "GitHub rejected the token (expired or wrong)."
    if status in (403, 404):
        return ("GitHub says the token can't reach that file - check the token has "
                "Contents read & write on the repo and that github_repo is right.")
    return f"GitHub returned {status}: {text[:150]}"
 
 
def file_exists(cfg: Dict, path: str) -> bool:
    """True if the file is in the repo (metadata only - nothing is downloaded)."""
    try:
        meta = requests.get(_contents_url(cfg, path), params={"ref": cfg["branch"]},
                            headers=_headers(cfg), timeout=60)
    except requests.RequestException as e:
        raise SyncError(f"couldn't reach GitHub ({e.__class__.__name__}).")
    if meta.status_code == 200:
        return True
    if meta.status_code == 404:
        repo = requests.get(f"{API}/repos/{cfg['repo']}", headers=_headers(cfg), timeout=60)
        if repo.status_code != 200:
            raise SyncError(_explain(repo.status_code, repo.text))
        return False
    raise SyncError(_explain(meta.status_code, meta.text))
 
 
def get_remote_file(cfg: Dict, path: str) -> Tuple[Optional[bytes], Optional[str]]:
    """(file bytes, sha) of a file in the repo, or (None, None) if it isn't there."""
    url = _contents_url(cfg, path)
    meta = requests.get(url, params={"ref": cfg["branch"]}, headers=_headers(cfg), timeout=60)
    if meta.status_code == 404:
        # 404 can mean "file not there yet" OR "token can't see the repo";
        # a repo-level check tells them apart.
        repo = requests.get(f"{API}/repos/{cfg['repo']}", headers=_headers(cfg), timeout=60)
        if repo.status_code != 200:
            raise SyncError(_explain(repo.status_code, repo.text))
        return None, None
    if meta.status_code != 200:
        raise SyncError(_explain(meta.status_code, meta.text))
    sha = meta.json()["sha"]
 
    raw = requests.get(url, params={"ref": cfg["branch"]}, headers=_headers(cfg, raw=True), timeout=120)
    if raw.status_code != 200:
        raise SyncError(_explain(raw.status_code, raw.text))
    return raw.content, sha
 
 
def pull_db(cfg: Dict, repo_path: str, local_path: str) -> Optional[str]:
    """Replace the local database with the copy saved in GitHub. Returns the
    sha to pass to push_db, or None if GitHub has no copy yet (the local file,
    if any, is left alone)."""
    try:
        data, sha = get_remote_file(cfg, repo_path)
    except requests.RequestException as e:
        raise SyncError(f"couldn't reach GitHub ({e.__class__.__name__}).")
    if data is None:
        return None
    if not data.startswith(SQLITE_MAGIC):
        raise SyncError(f"{repo_path} on GitHub isn't a database file.")
 
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(local_path) or ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, local_path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return sha
 
 
def push_db(cfg: Dict, repo_path: str, local_path: str, sha: Optional[str], message: str) -> str:
    """Upload the local database. `sha` is what pull_db returned (None when
    GitHub had no copy). Returns the new sha."""
    with open(local_path, "rb") as f:
        data = f.read()
    if not data.startswith(SQLITE_MAGIC):
        raise SyncError("the local database file looks damaged, so it was not uploaded.")
 
    body = {
        "message": message,
        "content": base64.b64encode(data).decode("ascii"),
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(_contents_url(cfg, repo_path), json=body, headers=_headers(cfg), timeout=180)
    except requests.RequestException as e:
        raise SyncError(f"couldn't reach GitHub ({e.__class__.__name__}).")
 
    if resp.status_code in (200, 201):
        return resp.json()["content"]["sha"]
    if resp.status_code in (409, 422):
        raise SyncError(
            "the database on GitHub changed while the check was running, so nothing was "
            "overwritten. Click the button again to start from the newest version."
        )
    raise SyncError(_explain(resp.status_code, resp.text))