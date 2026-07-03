# Changelog

Заметки об изменениях в маркетплейсе, по датам, а не по версиям — версии в манифестах намеренно не проставляются (см. README), актуальность обоих инструментов обеспечивается через коммиты. Файл нужен для истории: что и зачем менялось, — а не для семантического версионирования.

## 2026-07-03

### `mk-airflow` / `dag-review`

- Новые проверки в `analyze_dag.py`:
  - сенсор без `deferrable=True` или `mode='reschedule'` (держит worker-слот занятым весь poke-интервал);
  - устаревший `SubDagOperator` (deprecated с Airflow 2.0, предлагается TaskGroup);
  - устаревший ключ контекста `execution_date` (с Airflow 2.2 — `logical_date`/`data_interval_*`);
  - прямой доступ к metadata DB (`airflow.settings.Session`, `DagModel`/`TaskInstance`/`DagRun`);
  - разбросанные по DAG вызовы тасков/операторов вместо единого блока с зависимостями.
- `references/best-practices.md`: добавлены разделы про sensor modes, устаревшие паттерны и группировку тасков.
- Добавлены smoke-тесты (`tests/test_analyze_dag.py` + фикстуры) и GitHub Actions workflow, гоняющий их на каждый push/PR.

### `mk-airflow` / `debug`

- Пример подключения к Airflow передаёт пароль через переменную окружения, а не CLI-флагом (аргументы командной строки видны в истории шелла и `ps`).
- Исправлена нумерация чек-листа диагностики.
