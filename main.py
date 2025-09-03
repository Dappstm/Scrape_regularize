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
import os

api_key = "fa1fc5bae63538830211919b4878aec6"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s)


async def _launch_playwright() -> BrowserContext:
    logging.info("[CTX] Launching Playwright Chromium...")
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--disable-gpu",
            "--window-size=1366,768",
        ],
    )
    context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="pt-BR",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        extra_http_headers={
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Sec-Ch-Ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": "https://www.listadevedores.pgfn.gov.br/",
        },
        java_script_enabled=True,
        bypass_csp=True,
    )
    # Patch browser fingerprinting
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en'] });
    """)
    logging.info("[CTX] Chromium launched.")
    context._pw = pw  # type: ignore[attr-defined]
    context._browser = browser  # type: ignore[attr-defined]
    return context


async def _detect_hcaptcha_sitekey(page: Page, validate: bool = False) -> Optional[str]:
    """
    Returns the hCaptcha sitekey for www.listadevedores.pgfn.gov.br.
    Optionally validates the hardcoded sitekey by checking the page.

    Args:
        page: Playwright Page object representing the webpage.
        validate: If True, checks the page to ensure the hardcoded sitekey is present.

    Returns:
        Optional[str]: The hCaptcha sitekey, or None if validation fails and no sitekey is found.
    """
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # Hardcoded sitekey for www.listadevedores.pgfn.gov.br
    SITEKEY = "f8c1756d-a455-498f-94d4-05b16d8ad6b1"

    if not validate:
        logger.debug(f"Returning hardcoded sitekey: {SITEKEY}")
        return SITEKEY

    # Optional validation: Check if the hardcoded sitekey is present on the page
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
        logger.debug("Page loaded to networkidle state.")

        # Check for data-sitekey attribute
        detected_sitekey = await page.evaluate(
            """(sitekey) => {
                const el = document.querySelector(`[data-sitekey="${sitekey}"]`);
                return el ? el.getAttribute('data-sitekey') : null;
            }""",
            SITEKEY
        )

        if detected_sitekey == SITEKEY:
            logger.debug(f"Validated hardcoded sitekey: {SITEKEY}")
            return SITEKEY
        else:
            logger.warning(f"Hardcoded sitekey {SITEKEY} not found on page.")

    except Exception as e:
        return None


import random

async def _solve_hcaptcha_with_2captcha(page: Page, api_key: str, retries: int = 2) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Solve hCaptcha with 2Captcha, inject into Playwright page, and capture Authorization token.
    
    Args:
        page: Playwright Page object.
        api_key: 2Captcha API key.
        retries: Number of retry attempts.
    
    Returns:
        tuple[bool, Optional[str], Optional[str]]: (Success status, hCaptcha token, Authorization token).
    """
    content = (await page.content()).lower()
    if "hcaptcha" not in content and "captcha" not in content:
        logging.info("[HCAPTCHA] No captcha detected on %s — skipping solver.", page.url)
        return True, None, None

    sitekey = await _detect_hcaptcha_sitekey(page)
    if not sitekey:
        logging.warning("[HCAPTCHA] Could not detect hCaptcha sitekey at %s", page.url)
        return False, None, None

    solver = TwoCaptcha(api_key)
    attempt = 0
    token = None
    auth_token = None

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

            # Human-like behavior
            await page.wait_for_timeout(random.randint(5000, 10000))  # Random 5-10s delay
            # Move to hCaptcha widget (approximate coordinates)
            captcha = await page.query_selector("iframe[src*='hcaptcha.com']")
            if captcha:
                box = await captcha.bounding_box()
                if box:
                    x, y = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
                    await page.mouse.move(x, y, steps=15)
                    await page.mouse.click(x, y)
            else:
                # Fallback to generic movement
                await page.mouse.move(random.randint(200, 600), random.randint(200, 400), steps=15)
            await page.wait_for_timeout(random.randint(1000, 3000))

            formatted_token = token
            logging.info("[HCAPTCHA] Got token: %s, formatted: %s", token, formatted_token)

            # Inject token
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
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                        el.dispatchEvent(new Event('input', {bubbles: true}));
                    }
                    setTextarea('h-captcha-response', token);
                    setTextarea('g-recaptcha-response', token);
                }""",
                formatted_token,
            )
            await page.wait_for_timeout(random.randint(1500, 2500))

            # Capture Authorization token
            auth_token = await page.evaluate(
                "localStorage.getItem('auth_token') || sessionStorage.getItem('auth_token') || ''"
            )
            if auth_token:
                logging.info("[HCAPTCHA] Found Authorization token: Bearer %s", auth_token[:10] + "...")
            
            # Verify injection and cookies
            injected_token = await page.evaluate("document.querySelector('textarea[name=\"h-captcha-response\"]')?.value")
            cookies = await page.context.cookies()
            logging.debug("[HCAPTCHA] Cookies: %s", {c["name"]: c["value"] for c in cookies})
            if injected_token:
                logging.info("[HCAPTCHA] Token injected successfully: %s", injected_token)
                return True, injected_token, auth_token
            else:
                logging.error("[HCAPTCHA] Token injection failed (attempt %s).", attempt)
                continue

        except Exception as e:
            logging.error("[HCAPTCHA] 2Captcha attempt %s failed: %s", attempt, e, exc_info=True)

    logging.critical("[HCAPTCHA] All %s attempts failed. Cannot bypass captcha.", retries + 1)
    return False, None, None


async def run(query, out_dir, db_path, download_dir, two_captcha_key: Optional[str]):
    ctx: BrowserContext = None
    try:
        out_dir.mkdir(exist_ok=True, parents=True)
        db_engine = init_db(db_path)
        ctx = await _launch_playwright()

        # --- PGFN flow ---
        pgfn = PGFNClient(ctx)
        await pgfn.open()  # creates pgfn.page at PGFN_BASE

        auth_token = None
        if api_key:
            solved, auth_token = await _solve_hcaptcha_with_2captcha(pgfn.page, api_key, retries=2)
            if solved:
                logging.info("✅ hCaptcha solved for PGFN session")
            else:
                logging.warning("⚠️ Failed to solve hCaptcha, continuing anyway (may fail).")
        else:
            logging.info("[HCAPTCHA] No 2Captcha key provided, skipping solver.")

                # Get debtor rows directly from search_company
        debtors = await pgfn.search_company(query, auth_token=auth_token, max_retries=2)
        logging.info(f"Found {len(debtors)} debtor rows for '{query}'.")

        # Save and upsert into DB
        save_as_csv_json(debtors, out_dir)
        upsert_inscriptions(db_engine, [
            Inscription(
                cnpj=d.cnpj,
                company_name=d.company_name,
                inscription_number=None,  # not in DebtorRow
                category=None,            # not in DebtorRow
                amount=d.total,
            )
            for d in debtors
        ])

        # --- Regularize flow ---
        reg = RegularizeClient(ctx, download_dir)
        await reg.open()
        for d in debtors:
            try:
                # Regularize expects an inscription number, but DebtorRow doesn’t have it.
                # If DARF emission requires one, you’ll need to extend DebtorRow later.
                pdf_path = await reg.emitir_darf_integral(
                    only_digits(d.cnpj), None
                )
                logging.info(f"Saved DARF: {pdf_path}")
                link_darf(db_engine, d.cnpj, None, pdf_path)
            except Exception as err:
                logging.warning(f"DARF failed for {d.cnpj}: {err}")

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