# main.py
from __future__ import annotations
import argparse, re, logging, asyncio, os, sys
from pathlib import Path
from typing import Optional
from playwright.async_api import async_playwright, BrowserContext
from config import DEFAULT_OUT_DIR, DEFAULT_DB_PATH, DEFAULT_DOWNLOAD_DIR
from pgfn_client import PGFNClient
from regularize_client import RegularizeClient
from storage import Inscription, save_as_csv_json, init_db, upsert_inscriptions, link_darf

# Bright Data Scraping Browser over CDP
BRIGHTDATA_AUTH = os.getenv(
    "BRIGHTDATA_AUTH",
    "brd-customer-hl_d19d4367-zone-scraping_browser1:2f278pzcatsp"
)
SBR_WS_CDP = f"wss://{BRIGHTDATA_AUTH}@brd.superproxy.io:9222"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s)

async def _connect_brightdata() -> tuple[BrowserContext, object, object]:
    """
    Connect to Bright Data's Scraping Browser via CDP.
    Returns (context, browser, playwright) so we can cleanly close everything.
    """
    logging.info("[CTX] Connecting to Bright Data browser endpoint...")
    pw = await async_playwright().start()
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    logging.info("[CTX] Bright Data browser connected (hCaptcha bypass handled upstream).")
    return context, browser, pw

async def run(query: str, out_dir: Path, db_path: Path, download_dir: Path):
    ctx: Optional[BrowserContext] = None
    browser = None
    pw = None
    try:
        out_dir.mkdir(exist_ok=True, parents=True)
        db_engine = init_db(db_path)

        # --- Connect to Bright Data ---
        ctx, browser, pw = await _connect_brightdata()

        # --- PGFN flow ---
        pgfn = PGFNClient(ctx)
        await pgfn.open()

        # Perform search and get all CNPJs + inscriptions
        debtors = await pgfn.search_company(query)
        logging.info("Found %d debtor rows for '%s'.", len(debtors), query)

        # Save to CSV/JSON
        save_as_csv_json(debtors, out_dir)

        # Insert into DB
        all_inscriptions = []
        for d in debtors:
            for ins in (d.inscriptions or []):
                all_inscriptions.append(
                    Inscription(
                        cnpj=d.cnpj,
                        company_name=None,  # no longer available
                        inscription_number=ins,
                        category=None,
                        amount=None,
                    )
                )
        upsert_inscriptions(db_engine, all_inscriptions)

        # --- Regularize flow ---
        reg = RegularizeClient(ctx, download_dir)
        await reg.open()
        for d in debtors:
            for ins in (d.inscriptions or []):
                try:
                    pdf_path = await reg.emitir_darf_integral(
                        only_digits(d.cnpj), ins
                    )
                    logging.info(f"Saved DARF: {pdf_path}")
                    link_darf(db_engine, d.cnpj, ins, pdf_path)
                except Exception as err:
                    logging.warning(f"DARF failed for {d.cnpj} - {ins}: {err}")

    except Exception as main_err:
        logging.critical("[FATAL] Unhandled error in run(): %s", main_err, exc_info=True)
        sys.exit(1)
    finally:
        # Clean shutdown
        try:
            if ctx:
                await ctx.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass
        try:
            if pw:
                await pw.stop()
        except Exception:
            pass

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("pgfn_regularize.log", mode="a", encoding="utf-8")
        ]
    )

    parser = argparse.ArgumentParser("PGFN search + DARF via Bright Data (hCaptcha-free)")
    parser.add_argument("--query", required=True, help="Search company name")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--download-dir", default=str(DEFAULT_DOWNLOAD_DIR))
    args = parser.parse_args()

    asyncio.run(run(
        args.query,
        Path(args.out_dir),
        Path(args.db),
        Path(args.download_dir),
    ))