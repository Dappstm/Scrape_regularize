# main.py
from __future__ import annotations
import argparse, re, logging, asyncio, os, sys
from pathlib import Path
from typing import Optional
from playwright.async_api import async_playwright, BrowserContext, Page
from twocaptcha import TwoCaptcha
from config import DEFAULT_OUT_DIR, DEFAULT_DB_PATH, DEFAULT_DOWNLOAD_DIR, PGFN_BASE
from pgfn_client import PGFNClient
from regularize_client import RegularizeClient
from storage import Inscription, save_as_csv_json, init_db, upsert_inscriptions, link_darf

api_key = "fa1fc5bae63538830211919b4878aec6"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s)


async def _launch_playwright() -> BrowserContext:
    logging.info("[CTX] Launching Playwright Chromium...")
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--disable-gpu",
        ],
    )
    context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="pt-BR",
    )
    logging.info("[CTX] Chromium launched.")
    context._pw = pw  # type: ignore[attr-defined]
    context._browser = browser  # type: ignore[attr-defined]
    return context


async def _detect_hcaptcha_sitekey(page: Page) -> Optional[str]:
    sitekey = await page.evaluate(
        """() => {
            const el = document.querySelector('[data-sitekey]');
            return el ? el.getAttribute('data-sitekey') : null;
        }"""
    )
    if sitekey:
        return sitekey

    for fr in page.frames:
        try:
            url = fr.url or ""
            if "hcaptcha.com" in url:
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(url).query)
                if "sitekey" in qs and qs["sitekey"]:
                    return qs["sitekey"][0]
                if "k" in qs and qs["k"]:
                    return qs["k"][0]
        except Exception:
            pass
    return None


async def _solve_hcaptcha_with_2captcha(page: Page, api_key: str, retries: int = 2) -> bool:
    """Solve hCaptcha with 2Captcha and inject into Playwright page. Retries on failure."""
    content = (await page.content()).lower()
    if "hcaptcha" not in content and "captcha" not in content:
        logging.info("[HCAPTCHA] No captcha detected on %s — skipping solver.", page.url)
        return True

    sitekey = await _detect_hcaptcha_sitekey(page)
    if not sitekey:
        logging.warning("[HCAPTCHA] Could not detect hCaptcha sitekey at %s", page.url)
        return False

    solver = TwoCaptcha(api_key)

    attempt = 0
    while attempt <= retries:
        attempt += 1
        logging.info("[HCAPTCHA] Solving with 2Captcha (attempt %s/%s)...", attempt, retries + 1)
        try:
            result = await asyncio.to_thread(
                solver.hcaptcha,
                sitekey=sitekey,
                url=page.url,
            )
            token = result.get("code")
            if not token:
                logging.error("[HCAPTCHA] 2Captcha returned no token (attempt %s).", attempt)
                continue

            logging.info("[HCAPTCHA] Got token, injecting into page...")
            await page.evaluate(
                """(token) => {
                    function setTextarea(name, value) {
                        let el = document.querySelector('textarea[name="'+name+'"]');
                        if (!el) {
                            el = document.createElement('textarea');
                            el.name = name;
                            el.style.display = 'none';
                            document.body.appendChild(el);
                        }
                        el.value = value;
                        el.dispatchEvent(new Event('change', {bubbles:true}));
                        el.dispatchEvent(new Event('input', {bubbles:true}));
                    }
                    setTextarea('h-captcha-response', token);
                    setTextarea('g-recaptcha-response', token);
                }""",
                token,
            )
            await page.wait_for_timeout(1500)
            logging.info("[HCAPTCHA] Token injected successfully.")
            return True

        except Exception as e:
            logging.error("[HCAPTCHA] 2Captcha attempt %s failed: %s", attempt, e, exc_info=True)

    logging.critical("[HCAPTCHA] All %s attempts failed. Cannot bypass captcha.", retries + 1)
    return False


async def run(query, out_dir, db_path, download_dir, two_captcha_key: Optional[str]):
    ctx: BrowserContext = None
    try:
        out_dir.mkdir(exist_ok=True, parents=True)
        db_engine = init_db(db_path)
        ctx = await _launch_playwright()

        # --- PGFN flow ---
        pgfn = PGFNClient(ctx)
        await pgfn.open()  # creates pgfn.page at PGFN_BASE

        if two_captcha_key:
            solved = await _solve_hcaptcha_with_2captcha(pgfn.page, two_captcha_key, retries=2)
            if solved:
                logging.info("✅ hCaptcha solved for PGFN session.")
            else:
                logging.warning("⚠️ Failed to solve hCaptcha, continuing anyway (may fail).")
        else:
            logging.info("[HCAPTCHA] No 2Captcha key provided, skipping solver.")

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

        # --- Regularize flow ---
        reg = RegularizeClient(ctx, download_dir)
        await reg.open()
        for insc in inscriptions:
            try:
                pdf_path = await reg.emitir_darf_integral(
                    only_digits(insc.cnpj), insc.inscription_number
                )
                logging.info(f"Saved DARF: {pdf_path}")
                link_darf(db_engine, insc.cnpj, insc.inscription_number, pdf_path)
            except Exception as err:
                logging.warning(f"DARF failed for {insc.cnpj}/{insc.inscription_number}: {err}")

    except Exception as main_err:
        logging.critical("[FATAL] Unhandled error in run(): %s", main_err, exc_info=True)
        sys.exit(1)
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
            try:
                await ctx._browser.close()  # type: ignore[attr-defined]
            except Exception:
                pass
            try:
                await ctx._pw.stop()  # type: ignore[attr-defined]
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
    parser = argparse.ArgumentParser("PGFN/Regularize with 2Captcha hCaptcha bypass")
    parser.add_argument("--query", required=True, help="Search company name")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--download-dir", default=str(DEFAULT_DOWNLOAD_DIR))
    parser.add_argument("--captcha-key", default=os.getenv("TWO_CAPTCHA_KEY"),
                        help="2Captcha API key (or set TWO_CAPTCHA_KEY env var in Railway)")
    args = parser.parse_args()

    asyncio.run(run(
        args.query,
        Path(args.out_dir),
        Path(args.db),
        Path(args.download_dir),
        args.captcha_key,
    ))