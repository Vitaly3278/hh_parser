from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


SEARCH_URL = "https://hh.ru/search/vacancy"
SEARCH_QUERIES = (
    "Python разработчик",
    "Python программист",
    "Python developer",
    "Data Engineer Python",
    "Python",
)
REQUEST_TIMEOUT_SECONDS = 15
EXPERIENCE_QA_TO_LABEL = {
    "noExperience": "Без опыта",
    "between1And3": "1-3 года",
    "between3And6": "3-6 лет",
    "moreThan6": "Более 6 лет",
}
WORK_FORMAT_RULES = (
    ("Гибрид", ("гибрид",)),
    ("Удаленно", ("можно удал", "удален", "удалён", "remote")),
    ("Офис", ("офис", "на месте", "в офисе")),
)


def _clean_text(value: str) -> str:
    text_without_tags = re.sub(r"<[^>]*>", "", value)
    return " ".join(text_without_tags.split())


def _parse_salary_text(salary_text: str) -> tuple[int | None, int | None, str | None]:
    text = _clean_text(salary_text)
    normalized_text = text.replace("\u00a0", " ").replace("\u202f", " ")
    numbers = [int(num.replace(" ", "")) for num in re.findall(r"\d[\d ]*", normalized_text)]

    salary_from: int | None = None
    salary_to: int | None = None

    if len(numbers) >= 2:
        salary_from, salary_to = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if "до" in text.lower():
            salary_to = numbers[0]
        else:
            salary_from = numbers[0]

    currency: str | None = None
    if "₽" in normalized_text:
        currency = "RUR"
    elif "$" in normalized_text:
        currency = "USD"
    elif "€" in normalized_text:
        currency = "EUR"
    elif "₸" in normalized_text:
        currency = "KZT"
    elif "Br" in normalized_text:
        currency = "BYN"

    return salary_from, salary_to, currency


def _extract_hh_id(url: str) -> str | None:
    match = re.search(r"/vacancy/(\d+)", url)
    if not match:
        return None
    return match.group(1)


def _extract_snippet(card: Any) -> str:
    parts: list[str] = []
    for block in card.select('[class*="vacancy-snippet"]'):
        text = _clean_text(block.get_text(" ", strip=True))
        if text and text not in parts:
            parts.append(text)
    return " | ".join(parts)


def _detect_role(title: str, search_query: str) -> str:
    text = f"{title} {search_query}".lower()
    if "data engineer" in text or "дата инженер" in text:
        return "Data Engineer"
    if "программист" in text:
        return "Программист"
    return "Разработчик"


def _extract_experience(card: Any) -> str | None:
    experience_element = card.select_one('[data-qa^="vacancy-serp__vacancy-work-experience-"]')
    if not experience_element:
        return None
    qa_value = experience_element.get("data-qa", "")
    code = qa_value.replace("vacancy-serp__vacancy-work-experience-", "")
    return EXPERIENCE_QA_TO_LABEL.get(code)


def _extract_work_format(card: Any) -> str | None:
    card_text = _clean_text(card.get_text(" ", strip=True)).lower()
    for label, markers in WORK_FORMAT_RULES:
        if any(marker in card_text for marker in markers):
            return label
    return None


def _extract_salary_from_card(card: Any) -> tuple[int | None, int | None, str | None]:
    card_text = _clean_text(card.get_text(" ", strip=True))
    salary_match = re.search(
        r"(?:от|до)?\s*\d[\d\s\u00a0\u202f]*\s*(?:[–-]\s*\d[\d\s\u00a0\u202f]*\s*)?(?:₽|\$|€|₸|Br)",
        card_text,
        flags=re.IGNORECASE,
    )
    if salary_match:
        return _parse_salary_text(salary_match.group(0))
    return None, None, None


def fetch_vacancies_by_query(query: str, pages: int = 2, per_page: int = 50) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) hh-parser/1.0"}

    for page in range(pages):
        params = {
            "text": query,
            "page": page,
        }
        response = requests.get(
            SEARCH_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        cards = soup.select('[data-qa="vacancy-serp__vacancy"]')

        for card in cards[:per_page]:
            title_element = card.select_one('[data-qa="serp-item__title"]')
            if not title_element:
                continue

            title = _clean_text(title_element.get_text(" ", strip=True)) or "Без названия"
            url = urljoin("https://hh.ru", title_element.get("href", ""))
            hh_id = _extract_hh_id(url)
            if not hh_id:
                continue

            company_element = card.select_one('[data-qa="vacancy-serp__vacancy-employer-text"]')
            area_element = card.select_one('[data-qa="vacancy-serp__vacancy-address"]')
            salary_from, salary_to, currency = _extract_salary_from_card(card)

            found.append(
                {
                    "hh_id": hh_id,
                    "title": title,
                    "role": _detect_role(title=title, search_query=query),
                    "experience_required": _extract_experience(card),
                    "work_format": _extract_work_format(card),
                    "company": _clean_text(company_element.get_text(" ", strip=True))
                    if company_element
                    else "Не указано",
                    "salary_from": salary_from,
                    "salary_to": salary_to,
                    "currency": currency,
                    "area": _clean_text(area_element.get_text(" ", strip=True))
                    if area_element
                    else "Не указано",
                    "url": url,
                    "published_at": None,
                    "snippet": _extract_snippet(card),
                    "search_query": query,
                }
            )

    return found


def load_all_vacancies(
    pages_per_query: int = 2,
    per_page: int = 50,
) -> tuple[list[dict[str, Any]], list[str]]:
    merged: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    for query in SEARCH_QUERIES:
        try:
            vacancies = fetch_vacancies_by_query(query=query, pages=pages_per_query, per_page=per_page)
        except requests.RequestException as exc:
            errors.append(f"{query}: {exc}")
            continue

        for vacancy in vacancies:
            hh_id = vacancy["hh_id"]
            if hh_id not in merged:
                merged[hh_id] = vacancy

    return list(merged.values()), errors
