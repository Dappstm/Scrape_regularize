from pathlib import Path

# Base URLs
PGFN_BASE = "https://www.listadevedores.pgfn.gov.br"
REGULARIZE_BASE = "https://www.regularize.pgfn.gov.br"
REGULARIZE_DOC = f"{REGULARIZE_BASE}/docArrecadacao"

# Heuristics / URL fragments to watch in XHR
PGFN_JSON_HINTS = ["/api/devedores", "api/devedores", "devedores/", "devedores"]

# Output defaults
DEFAULT_OUT_DIR = Path("./out")
DEFAULT_DB_PATH = Path("./data.sqlite")
DEFAULT_DOWNLOAD_DIR = Path("./darfs")
DEFAULT_USER_DATA = Path("./user_data")  # Playwright persistent profile
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
STORAGE_PATH = Path("./playwright_state.json")

# Select browser
BROWSER = "chromium"  # choose from: chromium, firefox, webkit

# Timeouts (ms)
WAIT_LONG = 30_000
WAIT_MED = 10_000
WAIT_SHORT = 4_000
