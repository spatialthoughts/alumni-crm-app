"""Script to create test.db with fake data for testing the CRM interface."""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "test.db")

SCHEMA = """
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
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    course_id TEXT NOT NULL REFERENCES courses(id),
    batch_type TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS enrollments (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    batch_id TEXT NOT NULL REFERENCES batches(id),
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
    batch_id TEXT NOT NULL REFERENCES batches(id),
    certificate_id TEXT,
    certificate_url TEXT,
    issue_date TEXT,
    UNIQUE(person_id, batch_id)
);
"""

people = [
    (1, "Test", "User",        "test.user@example.com",    "+1-555-0001", "New York",   "United States", None,                        "Test account",          "Example Corp"),
    (2, "Fake", "McFakerson",  "fake@test.invalid",        "+1-555-0002", "London",     "United Kingdom","https://fake.example.com",  "Fake data for testing", "Fakery Inc"),
    (3, "Demo", "Person",      "demo@demo.test",           "+1-555-0003", "Bangalore",  "India",         None,                        None,                    "Demo Org"),
    (4, "Alice", "Placeholder","alice@placeholder.test",   "+1-555-0004", "Toronto",    "Canada",        None,                        "Fee waiver recipient",  "Sample University"),
    (5, "Bob",  "Example",     "bob.example@test.invalid", "+1-555-0005", "Sydney",     "Australia",     None,                        None,                    "Test Institute"),
]

people_emails = [
    (1, 1, "test.user@example.com",    1),
    (2, 1, "test.user.alt@example.com",0),
    (3, 2, "fake@test.invalid",        1),
    (4, 3, "demo@demo.test",           1),
    (5, 4, "alice@placeholder.test",   1),
    (6, 5, "bob.example@test.invalid", 1),
]

# Course and batch ids are text, as the website's are: a course_id and a cohort id.
courses = [
    ("gis_beginners",  "GIS for Beginners",    "Introduction to Geographic Information Systems"),
    ("spatial_analysis","Spatial Data Analysis","Advanced spatial data analysis techniques"),
]

batches = [
    ("gis_beginners_20240115",    "gis_beginners",   "public",  "2024-01-15", "2024-02-15", "Test batch 1"),
    ("gis_beginners_20240601",    "gis_beginners",   "private", "2024-06-01", "2024-06-30", "Test batch 2"),
    ("spatial_analysis_20240301", "spatial_analysis","public",  "2024-03-01", "2024-04-01", "Test batch 3"),
]

B1, B2, B3 = (b[0] for b in batches)

enrollments = [
    # (id, person_id, batch_id, attended, fee_waiver, organization, phone, domain, notes)
    (1, 1, B1, 1, None,        "Example Corp",       "+1-555-0001", "Technology",  None),
    (2, 2, B1, 1, None,        "Fakery Inc",         "+1-555-0002", "Education",   None),
    (3, 3, B1, 0, None,        "Demo Org",           "+1-555-0003", "NGO",         "Did not attend"),
    (4, 4, B2, 1, "scholarship","Sample University", "+1-555-0004", "Academia",    "Fee waiver granted"),
    (5, 5, B2, 1, None,        "Test Institute",     "+1-555-0005", "Research",    None),
    (6, 1, B3, 1, None,        "Example Corp",       "+1-555-0001", "Technology",  None),
    (7, 4, B3, 1, "discount",  "Sample University",  "+1-555-0004", "Academia",    None),
]

certifications = [
    # (id, person_id, batch_id, certificate_id, certificate_url, issue_date)
    (1, 1, B1, "TEST-001", "https://cert.example.com/TEST-001", "2024-02-20"),
    (2, 2, B1, "TEST-002", "https://cert.example.com/TEST-002", "2024-02-20"),
    (3, 4, B2, "TEST-003", "https://cert.example.com/TEST-003", "2024-07-10"),
    (4, 5, B2, "TEST-004", "https://cert.example.com/TEST-004", "2024-07-10"),
    (5, 1, B3, "TEST-005", "https://cert.example.com/TEST-005", "2024-04-10"),
]

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

con = sqlite3.connect(DB_PATH)
con.execute("PRAGMA foreign_keys = ON")
con.executescript(SCHEMA)

con.executemany("INSERT INTO people VALUES (?,?,?,?,?,?,?,?,?,?)", people)
con.executemany("INSERT INTO people_emails VALUES (?,?,?,?)", people_emails)
con.executemany("INSERT INTO courses VALUES (?,?,?)", courses)
con.executemany("INSERT INTO batches VALUES (?,?,?,?,?,?)", batches)
con.executemany("INSERT INTO enrollments VALUES (?,?,?,?,?,?,?,?,?)", enrollments)
con.executemany("INSERT INTO certifications VALUES (?,?,?,?,?,?)", certifications)

con.commit()
con.close()
print(f"Created {DB_PATH}")
