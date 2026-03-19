import sqlite3
from flask import g, current_app


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"],
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def dashboard_stats(db):
    return {
        "people": db.execute("SELECT COUNT(*) FROM people").fetchone()[0],
        "courses": db.execute("SELECT COUNT(*) FROM courses").fetchone()[0],
        "batches": db.execute("SELECT COUNT(*) FROM batches").fetchone()[0],
        "enrollments": db.execute("SELECT COUNT(*) FROM enrollments").fetchone()[0],
        "scholarships": db.execute(
            "SELECT COUNT(*) FROM enrollments WHERE fee_waiver IS NOT NULL AND fee_waiver != ''"
        ).fetchone()[0],
        "certs": db.execute(
            "SELECT COUNT(*) FROM certifications WHERE certificate_id IS NOT NULL AND certificate_id != ''"
        ).fetchone()[0],
    }


def search_people(
    db,
    q=None,
    course=None,
    waiver="any",
    has_cert="any",
    batch_type="any",
    country=None,
    start_date_from=None,
    start_date_to=None,
):
    conditions = []
    params = []
    sql = """
        SELECT DISTINCT p.id, p.first_name, p.last_name, p.primary_email, p.organization, p.city, p.country
        FROM people p
        LEFT JOIN enrollments e ON e.person_id = p.id
        LEFT JOIN batches b ON b.id = e.batch_id
        LEFT JOIN courses c ON c.id = b.course_id
        LEFT JOIN certifications cert ON cert.person_id = p.id AND cert.batch_id = e.batch_id
    """

    if q:
        conditions.append(
            "(p.first_name || ' ' || p.last_name LIKE ? OR p.primary_email LIKE ?)"
        )
        params += [f"%{q}%", f"%{q}%"]

    if course:
        conditions.append("c.id = ?")
        params.append(int(course))

    if waiver and waiver != "any":
        if waiver == "none":
            conditions.append("(e.fee_waiver IS NULL OR e.fee_waiver = '')")
        else:
            conditions.append("LOWER(e.fee_waiver) LIKE ?")
            params.append(f"%{waiver.lower()}%")

    if has_cert == "yes":
        conditions.append(
            "cert.certificate_id IS NOT NULL AND cert.certificate_id != ''"
        )
    elif has_cert == "no":
        conditions.append(
            "(cert.certificate_id IS NULL OR cert.certificate_id = '')"
        )

    if batch_type and batch_type != "any":
        conditions.append("b.batch_type = ?")
        params.append(batch_type)

    if country:
        conditions.append("p.country LIKE ?")
        params.append(f"%{country}%")

    if start_date_from:
        conditions.append("b.start_date >= ?")
        params.append(start_date_from)

    if start_date_to:
        conditions.append("b.start_date <= ?")
        params.append(start_date_to)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY p.first_name, p.last_name"

    return db.execute(sql, params).fetchall()


def get_person(db, person_id):
    person = db.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()
    if not person:
        return None, [], []

    enrollments = db.execute(
        """
        SELECT e.id, e.attended, e.fee_waiver, e.organization, e.phone, e.domain, e.notes,
               b.start_date, b.end_date, b.batch_type, b.id as batch_id,
               c.name as course_name, c.id as course_id,
               cert.certificate_id, cert.id as cert_id
        FROM enrollments e
        JOIN batches b ON b.id = e.batch_id
        JOIN courses c ON c.id = b.course_id
        LEFT JOIN certifications cert
            ON cert.person_id = e.person_id AND cert.batch_id = e.batch_id
        WHERE e.person_id = ?
        ORDER BY b.start_date DESC
        """,
        (person_id,),
    ).fetchall()

    emails = db.execute(
        "SELECT * FROM people_emails WHERE person_id = ? ORDER BY is_primary DESC",
        (person_id,),
    ).fetchall()

    return person, enrollments, emails
