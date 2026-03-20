#!/usr/bin/env python3
"""
CRM Import Script
Imports alumni enrollment CSVs into a normalized SQLite database.
Certificate IDs and fee_waiver data come directly from the enrollment CSVs.
"""

import sqlite3
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.dirname(BASE_DIR)  # parent dir holds the CSVs

DB_PATH = os.path.join(BASE_DIR, "crm.db")
PARTICIPANT_CSV = os.path.join(CSV_DIR, "participant_database.csv")

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    primary_email TEXT UNIQUE,
    primary_phone TEXT,
    city TEXT,
    country TEXT,
    social_media TEXT,
    notes TEXT,
    organization TEXT
);

CREATE TABLE IF NOT EXISTS people_emails (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    email TEXT UNIQUE NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS courses (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY,
    course_id INTEGER NOT NULL REFERENCES courses(id),
    batch_type TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    notes TEXT,
    UNIQUE(course_id, batch_type, start_date, end_date)
);

CREATE TABLE IF NOT EXISTS enrollments (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    attended INTEGER NOT NULL DEFAULT 0,
    fee_waiver TEXT,
    organization TEXT,
    phone TEXT,
    domain TEXT,
    notes TEXT,
    UNIQUE(person_id, batch_id)
);

CREATE TABLE IF NOT EXISTS certifications (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    certificate_id TEXT,
    certificate_url TEXT,
    issue_date TEXT,
    UNIQUE(person_id, batch_id)
);
"""


def parse_date(val):
    """Normalize date strings to ISO format (YYYY-MM-DD). Returns None if unparseable."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    val = str(val).strip()
    if not val:
        return None
    for fmt in ("%B %d,%Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return pd.to_datetime(val, format=fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return val


def clean_str(val):
    """Return stripped string or None for NaN/empty."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s else None


def upsert_course(cur, name):
    cur.execute("INSERT OR IGNORE INTO courses (name) VALUES (?)", (name,))
    cur.execute("SELECT id FROM courses WHERE name = ?", (name,))
    return cur.fetchone()[0]


def upsert_batch(cur, course_id, batch_type, start_date, end_date):
    cur.execute(
        """INSERT OR IGNORE INTO batches (course_id, batch_type, start_date, end_date)
           VALUES (?, ?, ?, ?)""",
        (course_id, batch_type, start_date, end_date),
    )
    cur.execute(
        "SELECT id FROM batches WHERE course_id=? AND batch_type=? AND start_date IS ? AND end_date IS ?",
        (course_id, batch_type, start_date, end_date),
    )
    return cur.fetchone()[0]


def upsert_person(cur, row):
    """Upsert person by primary_email. Returns person_id or None."""
    email = clean_str(row.get("primary_email"))
    if not email:
        return None

    first_name = clean_str(row.get("first_name"))
    last_name = clean_str(row.get("last_name"))
    phone = clean_str(row.get("primary_phone"))
    city = clean_str(row.get("city"))
    country = clean_str(row.get("country"))
    organization = clean_str(row.get("organization"))
    social_media = clean_str(row.get("social_media"))
    notes = clean_str(row.get("notes"))

    cur.execute("SELECT id FROM people WHERE primary_email = ?", (email,))
    existing = cur.fetchone()

    if existing:
        cur.execute(
            """UPDATE people SET
               first_name = COALESCE(?, first_name),
               last_name = COALESCE(?, last_name),
               primary_phone = COALESCE(?, primary_phone),
               city = COALESCE(?, city),
               country = COALESCE(?, country),
               organization = COALESCE(?, organization),
               social_media = COALESCE(?, social_media),
               notes = COALESCE(?, notes)
               WHERE id = ?""",
            (first_name, last_name, phone, city, country, organization, social_media, notes, existing[0]),
        )
        return existing[0]
    else:
        cur.execute(
            """INSERT INTO people (first_name, last_name, primary_email, primary_phone, city, country, organization, social_media, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (first_name, last_name, email, phone, city, country, organization, social_media, notes),
        )
        return cur.lastrowid


def ensure_email(cur, person_id, email, is_primary=0):
    """Add email to people_emails if not already there."""
    if not email:
        return
    cur.execute("SELECT id FROM people_emails WHERE email = ?", (email,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO people_emails (person_id, email, is_primary) VALUES (?, ?, ?)",
            (person_id, email, is_primary),
        )


def import_alumni(con, csv_path):
    print(f"\nImporting from {csv_path} ...")
    df = pd.read_csv(csv_path, dtype=str)
    df = df.dropna(axis=1, how="all")

    enrollment_count = 0
    cert_count = 0
    cur = con.cursor()

    for _, row in df.iterrows():
        course_name = clean_str(row.get("course"))
        email = clean_str(row.get("primary_email"))
        if not course_name or not email:
            continue

        batch_type = clean_str(row.get("batch_type")) or "public"
        start_date = parse_date(row.get("start_date"))
        end_date = parse_date(row.get("end_date"))
        attended_raw = clean_str(row.get("attended"))
        attended = 1 if attended_raw and attended_raw.strip().lower() in ("yes", "1", "true") else 0
        fee_waiver = clean_str(row.get("fee_waiver"))

        course_id = upsert_course(cur, course_name)
        batch_id = upsert_batch(cur, course_id, batch_type, start_date, end_date)
        person_id = upsert_person(cur, row)

        if person_id is None:
            continue

        ensure_email(cur, person_id, email, is_primary=1)

        org = clean_str(row.get("organization"))
        phone = clean_str(row.get("primary_phone"))
        domain = clean_str(row.get("domain"))
        notes = clean_str(row.get("notes"))

        cur.execute(
            """INSERT OR IGNORE INTO enrollments
               (person_id, batch_id, attended, fee_waiver, organization, phone, domain, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (person_id, batch_id, attended, fee_waiver, org, phone, domain, notes),
        )
        enrollment_count += 1

        cert_id = clean_str(row.get("certificate_id"))
        if cert_id:
            issue_date = parse_date(row.get("issue_date"))
            cur.execute(
                """INSERT OR IGNORE INTO certifications (person_id, batch_id, certificate_id, issue_date)
                   VALUES (?, ?, ?, ?)""",
                (person_id, batch_id, cert_id, issue_date),
            )
            cert_count += 1

    con.commit()
    print(f"  Done. {enrollment_count} rows processed, {cert_count} certs imported.")


def print_summary(con):
    cur = con.cursor()
    print("\n--- Import Summary ---")
    for table in ["people", "people_emails", "courses", "batches", "enrollments", "certifications"]:
        cur.execute(f"SELECT count(*) FROM {table}")
        print(f"  {table}: {cur.fetchone()[0]} rows")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA_SQL)
    con.commit()
    print(f"Created database: {DB_PATH}")

    if os.path.exists(PARTICIPANT_CSV):
        import_alumni(con, PARTICIPANT_CSV)
    else:
        print(f"\nWARNING: {PARTICIPANT_CSV} not found, skipping.")

    print_summary(con)
    con.close()
    print(f"\nDone. Run: python app.py")


if __name__ == "__main__":
    main()
