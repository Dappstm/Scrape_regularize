from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Iterable
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

@dataclass
class Inscription:
    cnpj: str
    company_name: str
    inscription_number: str
    category: str | None
    amount: float | None

def save_as_csv_json(inscriptions: List[Inscription], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(i) for i in inscriptions]
    df = pd.DataFrame(rows)
    df.to_csv(out_dir / "inscriptions.csv", index=False)
    df.to_json(out_dir / "inscriptions.json", orient="records", force_ascii=False, indent=2)

def init_db(db_path: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("""                CREATE TABLE IF NOT EXISTS inscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT,
                company_name TEXT,
                inscription_number TEXT,
                category TEXT,
                amount REAL
            );
        """))
        conn.execute(text("""                CREATE TABLE IF NOT EXISTS darfs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT,
                inscription_number TEXT,
                pdf_path TEXT
            );
        """))
    return engine

def upsert_inscriptions(engine, inscriptions: List[Inscription]):
    rows = [asdict(i) for i in inscriptions]
    if not rows:
        return
    cols = ["cnpj","company_name","inscription_number","category","amount"]
    placeholders = ",".join([":"+c for c in cols])
    sql = f"INSERT INTO inscriptions ({','.join(cols)}) VALUES ({placeholders})"
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def link_darf(engine, cnpj: str, inscription_number: str, pdf_path: Path):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO darfs (cnpj, inscription_number, pdf_path) VALUES (:c, :i, :p)"
        ), {"c": cnpj, "i": inscription_number, "p": str(pdf_path)})
