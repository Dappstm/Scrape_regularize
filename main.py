# main.py
from __future__ import annotations
import argparse, re, logging, asyncio
from pathlib import Path
from playwright.async_api import async_playwright, BrowserContext
from config import DEFAULT_OUT_DIR, DEFAULT_DB_PATH, DEFAULT_DOWNLOAD_DIR, BROWSER
from pgfn_client_async import PGFNClient
from regularize_client_async import RegularizeClient
from storage import Inscription, save_as_csv_json, init_db, upsert_inscriptions, link_darf

# BrightData credentials (move to environment in production)
AUTH = "brd-customer-hl_d19d4367-zone-scraping_browser1:2f278pzcatsp"
SBR_WS_CDP = f"wss://{AUTH}@brd.superproxy.io:9222"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s)

async def _connect_brightdata() -> BrowserContext:
    logging.info("[CTX] Connecting to BrightData browser endpoint...")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.connect_over_cdp(SBR_WS_CDP)
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    logging.info("[CTX] BrightData browser connected (hCaptcha will be handled).")
    return context

async def run(query, out_dir, db_path, download_dir):
    out_dir.mkdir(exist_ok=True, parents=True)
    db_engine = init_db(db_path)

    ctx = await _connect_brightdata()

    pgfn = PGFNClient(ctx)
    await pgfn.open()
    debtors = await pgfn.search_company(query)
    logging.info(f"Found {len(debtors)} debtor rows for '{query}'.")
    inscriptions = await pgfn.open_details_and_collect_inscriptions()
    logging.info(f"Collected {len(inscriptions)} inscriptions.")

    save_as_csv_json(inscriptions, out_dir)
    upsert_inscriptions(db_engine, [
        Inscription(
            cnpj=i.cnpj, company_name=i.company_name,
            inscription_number=i.inscription_number,
            category=i.category, amount=i.amount
        ) for i in inscriptions
    ])

    reg = RegularizeClient(ctx, download_dir)
    await reg.open()
    for insc in inscriptions:
        try:
            pdf_path = await reg.emitir_darf_integral(only_digits(insc.cnpj), insc.inscription_number)
            logging.info(f"Saved DARF: {pdf_path}")
            link_darf(db_engine, insc.cnpj, insc.inscription_number, pdf_path)
        except Exception as err:
            logging.warning(f"DARF failed for {insc.cnpj}/{insc.inscription_number}: {err}")

    await ctx.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser("PGFN/Regularize via BrightData")
    parser.add_argument("--query", required=True, help="Search company name")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--download-dir", default=str(DEFAULT_DOWNLOAD_DIR))
    args = parser.parse_args()

    asyncio.run(run(
        args.query, Path(args.out_dir),
        Path(args.db), Path(args.download_dir)
    ))