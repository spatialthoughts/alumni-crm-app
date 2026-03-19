import csv
import io
import os

from flask import (
    Flask,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)

import db as _db
from import_crm import (
    clean_str,
    ensure_email,
    parse_date,
    upsert_batch,
    upsert_course,
    upsert_person,
)

app = Flask(__name__)
app.config["DATABASE"] = os.environ.get(
    "CRM_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "crm.db"),
)

app.teardown_appcontext(_db.close_db)


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    database = _db.get_db()
    stats = _db.dashboard_stats(database)
    return render_template("index.html", stats=stats)


# ── API ───────────────────────────────────────────────────────────────────────

@app.route("/api/enrollments-by-country")
def api_enrollments_by_country():
    database = _db.get_db()
    rows = database.execute(
        """SELECT p.country, COUNT(e.id) as total
           FROM enrollments e
           JOIN people p ON p.id = e.person_id
           WHERE p.country IS NOT NULL AND p.country != ''
           GROUP BY p.country"""
    ).fetchall()
    from flask import jsonify
    return jsonify({r["country"]: r["total"] for r in rows})


# ── People ────────────────────────────────────────────────────────────────────

@app.route("/people")
def people():
    database = _db.get_db()
    q = request.args.get("q", "").strip() or None
    course = request.args.get("course", "").strip() or None
    waiver = request.args.get("waiver", "any")
    has_cert = request.args.get("has_cert", "any")
    batch_type = request.args.get("batch_type", "any")
    country = request.args.get("country", "").strip() or None

    results = _db.search_people(
        database, q=q, course=course, waiver=waiver,
        has_cert=has_cert, batch_type=batch_type, country=country,
    )
    courses = database.execute("SELECT id, name FROM courses ORDER BY name").fetchall()
    return render_template(
        "people.html", people=results, courses=courses,
        q=q or "", course=course or "", waiver=waiver,
        has_cert=has_cert, batch_type=batch_type, country=country or "",
    )


@app.route("/people/<int:pid>", methods=["GET", "POST"])
def person(pid):
    database = _db.get_db()
    if request.method == "POST":
        database.execute(
            """UPDATE people SET first_name=?, last_name=?, primary_phone=?, city=?,
               country=?, organization=?, social_media=?, notes=? WHERE id=?""",
            (
                request.form.get("first_name"),
                request.form.get("last_name"),
                request.form.get("phone"),
                request.form.get("city"),
                request.form.get("country"),
                request.form.get("organization") or None,
                request.form.get("social_media"),
                request.form.get("notes"),
                pid,
            ),
        )
        # Rebuild secondary emails: delete existing, re-insert submitted non-empty values
        database.execute("DELETE FROM people_emails WHERE person_id=? AND is_primary=0", (pid,))
        seen = set()
        primary = database.execute("SELECT primary_email FROM people WHERE id=?", (pid,)).fetchone()["primary_email"]
        for email in request.form.getlist("sec_email"):
            email = email.strip().lower()
            if email and email != primary.lower() and email not in seen:
                database.execute(
                    "INSERT OR IGNORE INTO people_emails (person_id, email, is_primary) VALUES (?,?,0)",
                    (pid, email),
                )
                seen.add(email)
        database.commit()
        return redirect(url_for("person", pid=pid))

    person_row, enrollments, emails = _db.get_person(database, pid)
    if not person_row:
        return "Person not found", 404
    return render_template(
        "person.html", person=person_row, enrollments=enrollments, emails=emails
    )


# ── Merge ─────────────────────────────────────────────────────────────────────

@app.route("/people/merge", methods=["POST"])
def merge_preview():
    ids = request.form.getlist("ids", type=int)
    if len(ids) != 2:
        return redirect(url_for("people"))
    database = _db.get_db()
    persons = []
    for pid in ids:
        person_row, enrollments, emails = _db.get_person(database, pid)
        if not person_row:
            return f"Person {pid} not found", 404
        persons.append({"person": person_row, "enrollments": enrollments, "emails": emails})
    return render_template("merge.html", persons=persons)


@app.route("/people/merge/confirm", methods=["POST"])
def merge_confirm():
    keep_id = request.form.get("keep_id", type=int)
    all_ids = request.form.getlist("all_ids", type=int)
    if not keep_id or len(all_ids) != 2 or keep_id not in all_ids:
        return "Invalid merge request", 400
    drop_id = next(i for i in all_ids if i != keep_id)

    database = _db.get_db()
    cur = database.cursor()

    # Move enrollments and certifications
    cur.execute("UPDATE enrollments SET person_id=? WHERE person_id=?", (keep_id, drop_id))
    cur.execute("UPDATE certifications SET person_id=? WHERE person_id=?", (keep_id, drop_id))

    # Absorb emails from drop into keep
    keep_primary = database.execute("SELECT primary_email FROM people WHERE id=?", (keep_id,)).fetchone()["primary_email"]
    drop_primary = database.execute("SELECT primary_email FROM people WHERE id=?", (drop_id,)).fetchone()["primary_email"]

    # Collect all emails belonging to drop (primary + any secondaries)
    drop_emails = [drop_primary] + [
        r["email"] for r in database.execute(
            "SELECT email FROM people_emails WHERE person_id=?", (drop_id,)
        ).fetchall()
    ]

    # Delete drop's people_emails rows first to free UNIQUE slots
    cur.execute("DELETE FROM people_emails WHERE person_id=?", (drop_id,))

    # Add each of drop's emails as secondary on keep (skip if already exists or equals keep's primary)
    for email in dict.fromkeys(drop_emails):  # deduplicate, preserve order
        if email.lower() == keep_primary.lower():
            continue
        cur.execute(
            "INSERT OR IGNORE INTO people_emails (person_id, email, is_primary) VALUES (?,?,0)",
            (keep_id, email),
        )

    # Fill any missing profile fields on keep from drop (non-destructive)
    cur.execute(
        """UPDATE people SET
           primary_phone   = COALESCE(primary_phone,   (SELECT primary_phone   FROM people WHERE id=?)),
           city            = COALESCE(city,            (SELECT city            FROM people WHERE id=?)),
           country         = COALESCE(country,         (SELECT country         FROM people WHERE id=?)),
           organization    = COALESCE(organization,    (SELECT organization    FROM people WHERE id=?)),
           social_media    = COALESCE(social_media,    (SELECT social_media    FROM people WHERE id=?)),
           notes           = COALESCE(notes,           (SELECT notes           FROM people WHERE id=?))
           WHERE id=?""",
        (drop_id, drop_id, drop_id, drop_id, drop_id, drop_id, keep_id),
    )

    cur.execute("DELETE FROM people WHERE id=?", (drop_id,))
    database.commit()

    return redirect(url_for("person", pid=keep_id))


# ── Courses ───────────────────────────────────────────────────────────────────

@app.route("/courses")
def courses():
    database = _db.get_db()
    rows = database.execute(
        """
        SELECT c.id, c.name, c.description,
               COUNT(DISTINCT b.id) as batch_count,
               COUNT(e.id) as enrollment_count
        FROM courses c
        LEFT JOIN batches b ON b.course_id = c.id
        LEFT JOIN enrollments e ON e.batch_id = b.id
        GROUP BY c.id
        ORDER BY c.name
        """
    ).fetchall()
    return render_template("courses.html", courses=rows)


@app.route("/courses/<int:cid>", methods=["GET", "POST"])
def course(cid):
    database = _db.get_db()
    if request.method == "POST":
        database.execute(
            "UPDATE courses SET name=?, description=? WHERE id=?",
            (request.form.get("name"), request.form.get("description"), cid),
        )
        database.commit()
        return redirect(url_for("course", cid=cid))

    course_row = database.execute("SELECT * FROM courses WHERE id=?", (cid,)).fetchone()
    if not course_row:
        return "Course not found", 404

    batches = database.execute(
        """
        SELECT b.*, COUNT(e.id) as enrollment_count
        FROM batches b
        LEFT JOIN enrollments e ON e.batch_id = b.id
        WHERE b.course_id = ?
        GROUP BY b.id
        ORDER BY b.start_date DESC
        """,
        (cid,),
    ).fetchall()
    return render_template("course.html", course=course_row, batches=batches)


# ── Batches ───────────────────────────────────────────────────────────────────

@app.route("/batches/<int:bid>", methods=["GET", "POST"])
def batch(bid):
    database = _db.get_db()
    if request.method == "POST":
        database.execute(
            "UPDATE batches SET start_date=?, end_date=?, notes=? WHERE id=?",
            (
                request.form.get("start_date"),
                request.form.get("end_date"),
                request.form.get("notes"),
                bid,
            ),
        )
        database.commit()
        return redirect(url_for("batch", bid=bid))

    batch_row = database.execute(
        """
        SELECT b.*, c.name as course_name, c.id as course_id
        FROM batches b JOIN courses c ON c.id = b.course_id
        WHERE b.id = ?
        """,
        (bid,),
    ).fetchone()
    if not batch_row:
        return "Batch not found", 404

    enrollments = database.execute(
        """
        SELECT e.id, e.attended, e.fee_waiver, e.organization,
               p.first_name, p.last_name, p.primary_email, p.id as person_id,
               cert.certificate_id
        FROM enrollments e
        JOIN people p ON p.id = e.person_id
        LEFT JOIN certifications cert
            ON cert.person_id = e.person_id AND cert.batch_id = e.batch_id
        WHERE e.batch_id = ?
        ORDER BY p.first_name, p.last_name
        """,
        (bid,),
    ).fetchall()
    return render_template("batch.html", batch=batch_row, enrollments=enrollments)


# ── Enrollments ───────────────────────────────────────────────────────────────

@app.route("/enrollments/<int:eid>", methods=["GET", "POST"])
def enrollment(eid):
    database = _db.get_db()

    if request.method == "POST":
        attended = 1 if request.form.get("attended") == "1" else 0
        database.execute(
            "UPDATE enrollments SET attended=?, fee_waiver=?, notes=? WHERE id=?",
            (attended, request.form.get("fee_waiver") or None, request.form.get("notes") or None, eid),
        )
        database.commit()

        cert_id_val = request.form.get("certificate_id", "").strip() or None
        enr = database.execute(
            "SELECT person_id, batch_id FROM enrollments WHERE id=?", (eid,)
        ).fetchone()
        if enr:
            existing_cert = database.execute(
                "SELECT id FROM certifications WHERE person_id=? AND batch_id=?",
                (enr["person_id"], enr["batch_id"]),
            ).fetchone()
            if existing_cert:
                database.execute(
                    "UPDATE certifications SET certificate_id=? WHERE id=?",
                    (cert_id_val, existing_cert["id"]),
                )
            elif cert_id_val:
                database.execute(
                    "INSERT INTO certifications (person_id, batch_id, certificate_id) VALUES (?,?,?)",
                    (enr["person_id"], enr["batch_id"], cert_id_val),
                )
            database.commit()
        return redirect(url_for("enrollment", eid=eid))

    row = database.execute(
        """
        SELECT e.id, e.attended, e.fee_waiver, e.organization, e.phone, e.domain, e.notes,
               p.first_name, p.last_name, p.id as person_id,
               b.start_date, b.end_date, b.batch_type, b.id as batch_id,
               c.name as course_name, c.id as course_id,
               cert.certificate_id, cert.id as cert_id
        FROM enrollments e
        JOIN people p ON p.id = e.person_id
        JOIN batches b ON b.id = e.batch_id
        JOIN courses c ON c.id = b.course_id
        LEFT JOIN certifications cert
            ON cert.person_id = e.person_id AND cert.batch_id = e.batch_id
        WHERE e.id = ?
        """,
        (eid,),
    ).fetchone()
    if not row:
        return "Enrollment not found", 404
    return render_template("enrollment.html", enrollment=row)


# ── Upload ────────────────────────────────────────────────────────────────────

@app.route("/upload/template.csv")
def upload_template():
    headers = [
        "First Name", "Last Name", "Course", "Email", "Secondary Email",
        "Phone", "City", "Country", "Organization", "Social Media",
        "Notes", "Domain", "Start Date", "End Date", "Attended",
        "Certificate ID", "fee_waiver",
    ]
    output = io.StringIO()
    csv.writer(output).writerow(headers)
    resp = make_response(output.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=template.csv"
    return resp


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "GET":
        return render_template("upload.html", result=None, error=None)

    f = request.files.get("csvfile")
    if not f or not f.filename:
        return render_template("upload.html", result=None, error="No file selected.")

    batch_type_upload = request.form.get("batch_type", "public")
    content = f.read().decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(content))

    database = _db.get_db()
    cur = database.cursor()
    new_people = 0
    new_enrollments = 0
    skipped = 0

    for row_dict in reader:
        course_name = clean_str(row_dict.get("Course"))
        email = clean_str(row_dict.get("Email"))
        if not course_name or not email:
            skipped += 1
            continue

        existed = database.execute(
            "SELECT id FROM people WHERE primary_email=?", (email,)
        ).fetchone()

        person_id = upsert_person(cur, row_dict)
        if not person_id:
            skipped += 1
            continue

        if not existed:
            new_people += 1

        ensure_email(cur, person_id, email, is_primary=1)
        sec_email = clean_str(row_dict.get("Secondary Email"))
        if sec_email:
            ensure_email(cur, person_id, sec_email, is_primary=0)

        start_date = parse_date(row_dict.get("Start Date"))
        end_date = parse_date(row_dict.get("End Date"))
        course_id = upsert_course(cur, course_name)
        batch_id = upsert_batch(cur, course_id, batch_type_upload, start_date, end_date)

        attended_raw = clean_str(row_dict.get("Attended"))
        attended = 1 if attended_raw and attended_raw.lower() in ("yes", "1", "true") else 0
        fee_waiver = clean_str(row_dict.get("fee_waiver"))
        org = clean_str(row_dict.get("Organization"))
        phone = clean_str(row_dict.get("Phone"))
        domain = clean_str(row_dict.get("Domain"))
        notes = clean_str(row_dict.get("Notes"))

        cur.execute(
            """INSERT OR IGNORE INTO enrollments
               (person_id, batch_id, attended, fee_waiver, organization, phone, domain, notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (person_id, batch_id, attended, fee_waiver, org, phone, domain, notes),
        )
        if cur.rowcount > 0:
            new_enrollments += 1
        else:
            skipped += 1

        cert_id_val = clean_str(row_dict.get("Certificate ID"))
        if cert_id_val:
            cur.execute(
                """INSERT OR IGNORE INTO certifications (person_id, batch_id, certificate_id)
                   VALUES (?,?,?)""",
                (person_id, batch_id, cert_id_val),
            )

    database.commit()
    return render_template(
        "upload.html",
        result={"new_people": new_people, "new_enrollments": new_enrollments, "skipped": skipped},
        error=None,
    )


# ── Query ─────────────────────────────────────────────────────────────────────

@app.route("/query")
def query():
    database = _db.get_db()
    q = request.args.get("q", "").strip() or None
    course = request.args.get("course", "").strip() or None
    waiver = request.args.get("waiver", "any")
    has_cert = request.args.get("has_cert", "any")
    batch_type = request.args.get("batch_type", "any")
    country = request.args.get("country", "").strip() or None
    start_date_from = request.args.get("start_date_from", "").strip() or None
    start_date_to = request.args.get("start_date_to", "").strip() or None
    export = request.args.get("export", "")
    preset = request.args.get("preset", "")

    # Apply presets
    if preset == "scholarship_cert":
        waiver = "scholarship"
        has_cert = "yes"
    elif preset == "discount":
        waiver = "discount"
    elif preset == "no_cert":
        has_cert = "no"
    elif preset == "multi_course":
        results = database.execute(
            """
            SELECT p.id, p.first_name, p.last_name, p.primary_email, p.city, p.country
            FROM people p
            WHERE (
                SELECT COUNT(DISTINCT b.course_id)
                FROM enrollments e JOIN batches b ON b.id = e.batch_id
                WHERE e.person_id = p.id
            ) > 1
            ORDER BY p.first_name, p.last_name
            """
        ).fetchall()
        courses = database.execute("SELECT id, name FROM courses ORDER BY name").fetchall()
        return render_template(
            "query.html", results=results, courses=courses,
            q="", course="", waiver="any", has_cert="any",
            batch_type="any", country="", start_date_from="", start_date_to="",
            preset=preset,
        )

    results = _db.search_people(
        database, q=q, course=course, waiver=waiver,
        has_cert=has_cert, batch_type=batch_type, country=country,
        start_date_from=start_date_from, start_date_to=start_date_to,
    )

    if export == "1":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "First Name", "Last Name", "Email", "City", "Country"])
        for r in results:
            writer.writerow([r["id"], r["first_name"], r["last_name"],
                             r["primary_email"], r["city"], r["country"]])
        resp = make_response(output.getvalue())
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = "attachment; filename=query_results.csv"
        return resp

    courses = database.execute("SELECT id, name FROM courses ORDER BY name").fetchall()
    return render_template(
        "query.html", results=results, courses=courses,
        q=q or "", course=course or "", waiver=waiver,
        has_cert=has_cert, batch_type=batch_type, country=country or "",
        start_date_from=start_date_from or "", start_date_to=start_date_to or "",
        preset=preset,
    )


if __name__ == "__main__":
    app.run(debug=True)
