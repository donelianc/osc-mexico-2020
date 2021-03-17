from tqdm import tqdm
from retry import retry
from pathlib import Path
from requests import Session
from functools import partial
from shutil import copyfileobj
from urllib3.util.retry import Retry
from urllib3.exceptions import ReadTimeoutError
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

# ===== from: https://stackoverflow.com/a/63831244 ===== #
# ===== from: https://stackoverflow.com/a/35504626 ===== #
@retry((ReadTimeoutError, Timeout), tries=5, delay=1, backoff=2, max_delay=20)
def pretty_download(url, path, log):
    s = Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[408, 500, 502, 503, 504, 599],
    )

    s.mount("http://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    try:
        r = s.get(url, headers=headers, stream=True, allow_redirects=True, timeout=5)
    except Timeout as out:
        log.info(
            "Server timeout (60 secods) after no response. Download process was canceled"
        )
        raise out

    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        code = f"Request to {url} returned status code {r.status_code}"
        log.info(code)
        raise RuntimeError(code)

    file_size = int(r.headers.get("Content-Length", 0))

    path = Path(path).expanduser().resolve()

    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = partial(r.raw.read, decode_content=True)  # Decompress if needed

    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with path.open("wb") as f:
            copyfileobj(r_raw, f)

    return str(path)