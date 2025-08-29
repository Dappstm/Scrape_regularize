from __future__ import annotations
import argparse, re, logging
from pathlib import Path
from playwright.sync_api import sync_playwright, BrowserContext
from config import (
    DEFAULT_OUT_DIR, DEFAULT_DB_PATH, DEFAULT_DOWNLOAD_DIR, 
    DEFAULT_USER_DATA, BROWSER, STORAGE_PATH
)
from pgfn_client import PGFNClient
from regularize_client import RegularizeClient
from storage import Inscription, save_as_csv_json, init_db, upsert_inscriptions, link_darf

_browser_ctx: BrowserContext | None = None

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s)

def _init_context(headless: bool, user_data: Path) -> BrowserContext:
    global _browser_ctx
    if _browser_ctx:
        return _browser_ctx

    with sync_playwright() as p:
        browser_type = getattr(p, BROWSER)
        logging.info("[CTX] Launching persistent Playwright context")
        _browser_ctx = browser_type.launch_persistent_context(
            user_data_dir=str(user_data),
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--start-maximized",
            ]
        )

    return _browser_ctx

def run(query: str, out_dir: Path, db_path: Path, download_dir: Path, user_data: Path, headless: bool=False):
    out_dir.mkdir(parents=True, exist_ok=True)
    db_engine = init_db(db_path)

    ctx = _init_context(headless, user_data)

    # --- PGFN ---
    pgfn = PGFNClient(ctx)
    pgfn.open()
    debtors = pgfn.search_company(query)
    print(f"Found {len(debtors)} potential debtor rows for query '{query}'.")
    inscriptions = pgfn.open_details_and_collect_inscriptions()
    print(f"Collected {len(inscriptions)} inscriptions.")

    # Persist inscriptions
    save_as_csv_json(inscriptions, out_dir)
    upsert_inscriptions(db_engine, [Inscription(
        cnpj=i.cnpj, company_name=i.company_name, inscription_number=i.inscription_number,
        category=i.category, amount=i.amount
    ) for i in inscriptions])

    # --- Regularize: emit DARFs for each inscription ---
    reg = RegularizeClient(ctx, download_dir=download_dir)
    reg.open()
    for insc in inscriptions:
        cnpj_digits = only_digits(insc.cnpj)
        try:
            pdf_path = reg.emitir_darf_integral(cnpj_digits, insc.inscription_number)
            print(f"Saved DARF: {pdf_path}")
            link_darf(db_engine, insc.cnpj, insc.inscription_number, pdf_path)
        except Exception as e:
            print(f"WARN: Could not issue DARF for {insc.cnpj} / {insc.inscription_number}: {e}")

    ctx.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PGFN/Regularize mini-program (Playwright persistent context)")
    ap.add_argument("--query", required=True, help="Company name to search (e.g., 'viacao aerea sao paulo')")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH))
    ap.add_argument("--download-dir", default=str(DEFAULT_DOWNLOAD_DIR))
    ap.add_argument("--user-data", default=str(DEFAULT_USER_DATA), help="Persistent browser profile directory")
    ap.add_argument("--headless", action="store_true", help="Run headless (NOT recommended if captcha appears)")
    args = ap.parse_args()
    run(
        query=args.query,
        out_dir=Path(args.out_dir),
        db_path=Path(args.db),
        download_dir=Path(args.download_dir),
        user_data=Path(args.user_data),
        headless=args.headless
    )