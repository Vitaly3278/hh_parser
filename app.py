from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import Flask, flash, redirect, render_template, request, url_for

from database import (
    get_analytics_data,
    get_favorite_vacancies,
    get_filter_options,
    get_stats,
    get_vacancies,
    init_db,
    save_vacancies,
    toggle_favorite,
)
from parser_hh import load_all_vacancies


app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-in-production"
init_db()
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
LAST_RUN_NEW = 0


def format_salary(salary_from: int | None, salary_to: int | None, currency: str | None) -> str:
    if salary_from is None and salary_to is None:
        return "Не указана"
    currency_label = currency or ""
    if salary_from is not None and salary_to is not None:
        return f"{salary_from:,} - {salary_to:,} {currency_label}".replace(",", " ")
    if salary_from is not None:
        return f"от {salary_from:,} {currency_label}".replace(",", " ")
    return f"до {salary_to:,} {currency_label}".replace(",", " ")


def format_db_datetime(value: str | None) -> str:
    if not value:
        return "дата не указана"
    try:
        parsed_utc = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return value
    try:
        parsed_moscow = parsed_utc.astimezone(MOSCOW_TZ)
    except Exception:  # noqa: BLE001
        parsed_moscow = parsed_utc + timedelta(hours=3)
    return parsed_moscow.strftime("%d.%m.%Y %H.%M")


def format_int(value: int | None) -> str:
    if value is None:
        return "Не указано"
    return f"{value:,}".replace(",", " ")


@app.context_processor
def utility_context() -> dict[str, object]:
    return {
        "format_salary": format_salary,
        "format_db_datetime": format_db_datetime,
        "format_int": format_int,
    }


@app.route("/", methods=["GET"])
def index():
    search_text = request.args.get("q", "").strip()
    role = request.args.get("role", "").strip()
    experience = request.args.get("experience", "").strip()
    work_format = request.args.get("work_format", "").strip()
    vacancies = get_vacancies(
        limit=300,
        search=search_text or None,
        role=role or None,
        experience=experience or None,
        work_format=work_format or None,
    )
    stats = get_stats()
    filter_options = get_filter_options()
    current_url = request.full_path if request.query_string else request.path
    return render_template(
        "index.html",
        vacancies=vacancies,
        stats=stats,
        search_text=search_text,
        selected_role=role,
        selected_experience=experience,
        selected_work_format=work_format,
        role_options=filter_options["roles"],
        experience_options=filter_options["experiences"],
        work_format_options=filter_options["work_formats"],
        last_run_new=LAST_RUN_NEW,
        current_url=current_url,
    )


@app.route("/update", methods=["POST"])
def update_data():
    global LAST_RUN_NEW
    LAST_RUN_NEW = 0
    try:
        vacancies, errors = load_all_vacancies(pages_per_query=2, per_page=50)
        save_result = save_vacancies(vacancies)
        added = save_result["added"]
        updated = save_result["updated"]
        LAST_RUN_NEW = added
        if errors and not vacancies:
            flash(
                "Не удалось получить вакансии с hh.ru. "
                "Вероятно, API временно ограничивает доступ с текущего IP.",
                "danger",
            )
        else:
            print(f"Новых вакансий за запуск: {added}")
            flash(
                "Обновление завершено: "
                f"загружено {len(vacancies)} вакансий, "
                f"добавлено новых {added}, обновлено {updated}.",
                "success",
            )
            if errors:
                flash(
                    "Часть запросов не обработана: " + "; ".join(errors[:2]),
                    "warning",
                )
    except Exception as exc:  # noqa: BLE001
        flash(f"Ошибка при загрузке вакансий: {exc}", "danger")
    return redirect(url_for("index"))


@app.route("/analytics", methods=["GET"])
def analytics():
    analytics_data = get_analytics_data()
    return render_template("analytics.html", analytics=analytics_data)


@app.route("/favorites", methods=["GET"])
def favorites():
    favorites_list = get_favorite_vacancies(limit=500)
    stats = get_stats()
    current_url = request.path
    return render_template(
        "favorites.html",
        vacancies=favorites_list,
        stats=stats,
        current_url=current_url,
    )


@app.route("/favorite/<hh_id>", methods=["POST"])
def favorite_toggle(hh_id: str):
    is_favorite = toggle_favorite(hh_id)
    flash(
        "Вакансия добавлена в избранное." if is_favorite else "Вакансия удалена из избранного.",
        "success",
    )
    next_url = request.form.get("next_url", "").strip()
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)
