# PGFN / Regularize Mini-Program (Playwright, manual hCaptcha solve)

This compact program uses **Playwright** (Python) to:

1) Query **PGFN Lista de Devedores** for a company name, collect CNPJs and debt inscriptions.
2) Extract structured data (CNPJ, inscription number, category, amount, etc.).
3) Access the "Detalhar" view to capture all inscriptions/values per CNPJ.
4) Store results as CSV/JSON and in SQLite.
5) Issue **DARF(s)** on **Regularize** using each (CNPJ, inscrição) and save the resulting PDF.

**Important:** The PGFN site uses hCaptcha. This program **does not bypass** it.
It **solves** it.

---

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
playwright install-deps
playwright install chromium

```

Run a search and issue DARFs for all inscriptions found:

```bash
python main.py --query "viacao aerea sao paulo"    #--download-dir ./darfs --db ./data.sqlite --out-dir ./out
```


- The script will:
  - Search by company name in **Lista de Devedores**
  - Capture JSON/XHR for results and details (after captcha solved)
  - Save structured data to CSV/JSON and SQLite
  - Visit **Regularize** and issue DARFs (PDF) for each inscription, saving them into `--download-dir`

To re-run searches without solving captcha again, reuse the persistent profile via `--user-data` (default `./user_data`).

```bash
python main.py --query "viacao aerea sao paulo" --user-data ./user_data
```

---

## Files

- `config.py` — constants and paths
- `pgfn_client.py` — search + details from **Lista de Devedores**, network capture and JSON parsing
- `regularize_client.py` — issue DARFs in **Regularize** and save PDFs
- `storage.py` — CSV/JSON/SQLite persistence helpers
- `main.py` — orchestration CLI
- `requirements.txt` — dependencies

---

## Notes on Network Interception

Both portals are SPA/React apps. After you solve hCaptcha on PGFN, the SPA issues XHR calls like:

- `/api/devedores?id=...` (example) to fetch debtor details
- Regularize emits PDF (`content-type: application/pdf`) after **Emitir DARF integral → Imprimir**

This program listens to `page.on("response")` and captures JSON/PDF bodies when URLs match expected patterns.
JSON is parsed and merged into structured Python objects; PDFs are written to disk and linked by (CNPJ, inscrição).

---

## Legal & Compliance

- This tool **respects hCaptcha** and never attempts to bypass it.
- You operate it in a real browser session, in line with the portals' requirements.
- Use responsibly and in accordance with applicable laws and site terms.
