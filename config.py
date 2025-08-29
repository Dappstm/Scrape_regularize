from pathlib import Path

# Base URLs
PGFN_BASE = "https://www.listadevedores.pgfn.gov.br"
REGULARIZE_BASE = "https://www.regularize.pgfn.gov.br"
REGULARIZE_DOC = f"{REGULARIZE_BASE}/docArrecadacao"

# Heuristics / URL fragments to watch in XHR
PGFN_JSON_HINTS = ["/api/devedores", "/api", "/consulta", "/devedores"]

# Output defaults
DEFAULT_OUT_DIR = Path("./out")
DEFAULT_DB_PATH = Path("./data.sqlite")
DEFAULT_DOWNLOAD_DIR = Path("./darfs")
DEFAULT_USER_DATA = Path("./user_data")  # Playwright persistent profile

# Select browser
BROWSER = "chromium"  # choose from: chromium, firefox, webkit

# Timeouts (ms)
WAIT_LONG = 30_000
WAIT_MED = 10_000
WAIT_SHORT = 4_000
