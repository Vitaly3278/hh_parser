from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


DB_PATH = Path("vacancies.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hh_id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                role TEXT,
                experience_required TEXT,
                work_format TEXT,
                is_favorite INTEGER DEFAULT 0,
                company TEXT NOT NULL,
                salary_from INTEGER,
                salary_to INTEGER,
                currency TEXT,
                area TEXT,
                url TEXT NOT NULL,
                published_at TEXT,
                snippet TEXT,
                search_query TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "vacancies", "role", "TEXT")
        _ensure_column(conn, "vacancies", "experience_required", "TEXT")
        _ensure_column(conn, "vacancies", "work_format", "TEXT")
        _ensure_column(conn, "vacancies", "is_favorite", "INTEGER DEFAULT 0")
        conn.commit()


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql_type: str,
) -> None:
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_names = {column["name"] for column in columns}
    if column_name in existing_names:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql_type}")


def save_vacancies(vacancies: list[dict[str, Any]]) -> dict[str, int]:
    added = 0
    updated = 0
    with get_connection() as conn:
        for vacancy in vacancies:
            insert_cursor = conn.execute(
                """
                INSERT OR IGNORE INTO vacancies (
                    hh_id,
                    title,
                    role,
                    experience_required,
                    work_format,
                    company,
                    salary_from,
                    salary_to,
                    currency,
                    area,
                    url,
                    published_at,
                    snippet,
                    search_query
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    vacancy["hh_id"],
                    vacancy["title"],
                    vacancy["role"],
                    vacancy["experience_required"],
                    vacancy["work_format"],
                    vacancy["company"],
                    vacancy["salary_from"],
                    vacancy["salary_to"],
                    vacancy["currency"],
                    vacancy["area"],
                    vacancy["url"],
                    vacancy["published_at"],
                    vacancy["snippet"],
                    vacancy["search_query"],
                ),
            )

            if insert_cursor.rowcount == 1:
                added += 1
                continue

            update_cursor = conn.execute(
                """
                UPDATE vacancies
                SET
                    title = ?,
                    role = ?,
                    experience_required = ?,
                    work_format = ?,
                    company = ?,
                    salary_from = ?,
                    salary_to = ?,
                    currency = ?,
                    area = ?,
                    url = ?,
                    published_at = ?,
                    snippet = ?,
                    search_query = ?
                WHERE hh_id = ?
                """,
                (
                    vacancy["title"],
                    vacancy["role"],
                    vacancy["experience_required"],
                    vacancy["work_format"],
                    vacancy["company"],
                    vacancy["salary_from"],
                    vacancy["salary_to"],
                    vacancy["currency"],
                    vacancy["area"],
                    vacancy["url"],
                    vacancy["published_at"],
                    vacancy["snippet"],
                    vacancy["search_query"],
                    vacancy["hh_id"],
                ),
            )
            updated += update_cursor.rowcount
        conn.commit()
    return {"added": added, "updated": updated}


def get_vacancies(
    limit: int = 200,
    search: str | None = None,
    role: str | None = None,
    experience: str | None = None,
    work_format: str | None = None,
) -> list[sqlite3.Row]:
    query = """
        SELECT
            hh_id,
            title,
            role,
            experience_required,
            work_format,
            is_favorite,
            company,
            salary_from,
            salary_to,
            currency,
            area,
            url,
            published_at,
            created_at,
            snippet,
            search_query
        FROM vacancies
    """
    params: list[Any] = []
    conditions: list[str] = []

    if search:
        conditions.append(
            """
                (
                title LIKE ?
                OR company LIKE ?
                OR area LIKE ?
                OR snippet LIKE ?
                OR search_query LIKE ?
                )
            """
        )
        like_value = f"%{search}%"
        params.extend([like_value] * 5)

    if role:
        conditions.append("role = ?")
        params.append(role)

    if experience:
        conditions.append("experience_required = ?")
        params.append(experience)

    if work_format:
        conditions.append("work_format = ?")
        params.append(work_format)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY datetime(created_at) DESC, id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return rows


def get_stats() -> dict[str, int]:
    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) AS value FROM vacancies").fetchone()["value"]
        with_salary = conn.execute(
            """
            SELECT COUNT(*) AS value
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
            """
        ).fetchone()["value"]
        unique_companies = conn.execute(
            "SELECT COUNT(DISTINCT company) AS value FROM vacancies"
        ).fetchone()["value"]
        favorite_count = conn.execute(
            "SELECT COUNT(*) AS value FROM vacancies WHERE is_favorite = 1"
        ).fetchone()["value"]
    return {
        "total": total,
        "with_salary": with_salary,
        "unique_companies": unique_companies,
        "favorite_count": favorite_count,
    }


def get_filter_options() -> dict[str, list[str]]:
    with get_connection() as conn:
        roles = [
            row["value"]
            for row in conn.execute(
                """
                SELECT DISTINCT role AS value
                FROM vacancies
                WHERE role IS NOT NULL AND role != ''
                ORDER BY role
                """
            ).fetchall()
        ]
        experiences = [
            row["value"]
            for row in conn.execute(
                """
                SELECT DISTINCT experience_required AS value
                FROM vacancies
                WHERE experience_required IS NOT NULL AND experience_required != ''
                ORDER BY
                    CASE experience_required
                        WHEN 'Без опыта' THEN 1
                        WHEN '1-3 года' THEN 2
                        WHEN '3-6 лет' THEN 3
                        WHEN 'Более 6 лет' THEN 4
                        ELSE 5
                    END
                """
            ).fetchall()
        ]
        work_formats = [
            row["value"]
            for row in conn.execute(
                """
                SELECT DISTINCT work_format AS value
                FROM vacancies
                WHERE work_format IS NOT NULL AND work_format != ''
                ORDER BY
                    CASE work_format
                        WHEN 'Удаленно' THEN 1
                        WHEN 'Гибрид' THEN 2
                        WHEN 'Офис' THEN 3
                        ELSE 4
                    END
                """
            ).fetchall()
        ]
    return {"roles": roles, "experiences": experiences, "work_formats": work_formats}


def get_analytics_data() -> dict[str, Any]:
    with get_connection() as conn:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT company) AS unique_companies,
                SUM(CASE WHEN salary_from IS NOT NULL OR salary_to IS NOT NULL THEN 1 ELSE 0 END) AS with_salary,
                AVG(
                    CASE
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                        ELSE NULL
                    END
                ) AS avg_salary
            FROM vacancies
            """
        ).fetchone()

        by_role = conn.execute(
            """
            SELECT COALESCE(role, 'Не указано') AS label, COUNT(*) AS value
            FROM vacancies
            GROUP BY COALESCE(role, 'Не указано')
            ORDER BY value DESC
            """
        ).fetchall()

        by_experience = conn.execute(
            """
            SELECT COALESCE(experience_required, 'Не указано') AS label, COUNT(*) AS value
            FROM vacancies
            GROUP BY COALESCE(experience_required, 'Не указано')
            ORDER BY
                CASE COALESCE(experience_required, 'Не указано')
                    WHEN 'Без опыта' THEN 1
                    WHEN '1-3 года' THEN 2
                    WHEN '3-6 лет' THEN 3
                    WHEN 'Более 6 лет' THEN 4
                    ELSE 5
                END
            """
        ).fetchall()

        by_work_format = conn.execute(
            """
            SELECT COALESCE(work_format, 'Не указано') AS label, COUNT(*) AS value
            FROM vacancies
            GROUP BY COALESCE(work_format, 'Не указано')
            ORDER BY
                CASE COALESCE(work_format, 'Не указано')
                    WHEN 'Удаленно' THEN 1
                    WHEN 'Гибрид' THEN 2
                    WHEN 'Офис' THEN 3
                    ELSE 4
                END
            """
        ).fetchall()

        top_companies = conn.execute(
            """
            SELECT company, COUNT(*) AS value
            FROM vacancies
            GROUP BY company
            ORDER BY value DESC, company ASC
            LIMIT 10
            """
        ).fetchall()

        by_day = conn.execute(
            """
            SELECT
                date(datetime(created_at, '+3 hours')) AS day,
                COUNT(*) AS value
            FROM vacancies
            GROUP BY day
            ORDER BY day DESC
            LIMIT 14
            """
        ).fetchall()

    by_day_desc = list(by_day)
    by_day_asc = list(reversed(by_day_desc))

    return {
        "totals": {
            "total": totals["total"] or 0,
            "unique_companies": totals["unique_companies"] or 0,
            "with_salary": totals["with_salary"] or 0,
            "avg_salary": int(totals["avg_salary"]) if totals["avg_salary"] is not None else None,
        },
        "by_role": [{"label": row["label"], "value": row["value"]} for row in by_role],
        "by_experience": [{"label": row["label"], "value": row["value"]} for row in by_experience],
        "by_work_format": [{"label": row["label"], "value": row["value"]} for row in by_work_format],
        "top_companies": [{"label": row["company"], "value": row["value"]} for row in top_companies],
        "by_day": [{"label": row["day"], "value": row["value"]} for row in by_day_asc],
    }


def toggle_favorite(hh_id: str) -> bool:
    with get_connection() as conn:
        current = conn.execute(
            "SELECT is_favorite FROM vacancies WHERE hh_id = ?",
            (hh_id,),
        ).fetchone()
        if current is None:
            return False
        new_value = 0 if current["is_favorite"] == 1 else 1
        conn.execute(
            "UPDATE vacancies SET is_favorite = ? WHERE hh_id = ?",
            (new_value, hh_id),
        )
        conn.commit()
    return bool(new_value)


def get_favorite_vacancies(limit: int = 300) -> list[sqlite3.Row]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                hh_id,
                title,
                role,
                experience_required,
                work_format,
                is_favorite,
                company,
                salary_from,
                salary_to,
                currency,
                area,
                url,
                published_at,
                created_at,
                snippet,
                search_query
            FROM vacancies
            WHERE is_favorite = 1
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return rows
