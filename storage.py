# storage.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

@dataclass
class Inscription:
    cnpj: str
    inscription_number: str

def save_as_csv_json(inscriptions: List[Inscription], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(i) for i in inscriptions]
    df = pd.DataFrame(rows)
    df.to_csv(out_dir / "inscriptions.csv", index=False)
    df.to_json(out_dir / "inscriptions.json", orient="records", force_ascii=False, indent=2)

def init_db(db_path: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT NOT NULL,
                inscription_number TEXT NOT NULL,
                UNIQUE(cnpj, inscription_number)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS darfs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT NOT NULL,
                inscription_number TEXT,
                pdf_path TEXT
            );
        """))
    return engine

def upsert_inscriptions(engine, inscriptions: List[Inscription]):
    rows = [asdict(i) for i in inscriptions]
    if not rows:
        return

    sql = """
        INSERT INTO inscriptions (cnpj, inscription_number)
        VALUES (:cnpj, :inscription_number)
        ON CONFLICT(cnpj, inscription_number) DO NOTHING
    """
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def link_darf(engine, cnpj: str, inscription_number: str, pdf_path: Path):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO darfs (cnpj, inscription_number, pdf_path) VALUES (:c, :i, :p)",
        ), {"c": cnpj, "i": inscription_number, "p": str(pdf_path)})