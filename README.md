# HH Parser вакансий на Python

Приложение собирает вакансии с hh.ru по ролям:
- Python разработчик
- Python программист
- Python developer
- Data Engineer Python

Далее сохраняет данные в SQLite и показывает их на веб-интерфейсе.

## Запуск

1. Установите зависимости:

```bash
pip install -r requirements.txt
```

2. Запустите приложение:

```bash
python app.py
```

3. Откройте в браузере:

`http://127.0.0.1:5000`

4. Нажмите кнопку **"Обновить вакансии из hh.ru"**.

## Что внутри

- `parser_hh.py` — загрузка вакансий из API hh.ru
- `database.py` — работа с SQLite
- `app.py` — Flask-приложение
- `templates/index.html` — web-интерфейс
