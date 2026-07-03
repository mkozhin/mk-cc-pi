# Airflow 2.x DAG Best Practices — Справочник

Используй этот файл для углублённых объяснений в отчёте.
Источники: [Astronomer Docs](https://www.astronomer.io/docs/learn/dag-best-practices), [Apache Airflow Docs](https://airflow.apache.org/docs/).

---

## 1. Идемпотентность (Idempotency)

**Что это:** DAG идемпотентен если повторный запуск с теми же параметрами даёт тот же результат.

**Почему важно:** Airflow активно использует ретраи. Если задача не идемпотентна — ретрай может записать данные дважды, или использовать неверную дату.

**Антипаттерны:**
```python
# BAD: datetime.now() на уровне модуля
today = datetime.now()

# BAD: INSERT без проверки на дубли
cursor.execute("INSERT INTO table VALUES (...)")

# GOOD: Jinja-шаблоны
# {{ ds }} — дата запуска (YYYY-MM-DD)
# {{ data_interval_start }} — начало интервала
# {{ prev_start_date_success }} — дата последнего успешного запуска
```

**Паттерн для инкрементальной загрузки:**
```python
# GOOD: UPSERT вместо INSERT
cursor.execute("""
    INSERT INTO table (id, value, updated_at)
    VALUES (%(id)s, %(value)s, %(updated_at)s)
    ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
""", row)
```

---

## 2. Top-level код (No top-level code)

**Что это:** Любой код вне функций/классов на уровне модуля.

**Почему важно:** Airflow-scheduler парсит каждый DAG-файл каждые `min_file_process_interval` секунд (по умолчанию 30 с). Тяжёлый top-level код:
- Замедляет scheduler для всего кластера
- Вызывает ошибки если внешний сервис недоступен (весь DAG падает при парсинге)
- Создаёт утечки соединений

**Антипаттерн:**
```python
# BAD: Variable.get при каждом парсинге
config = Variable.get("my_config", deserialize_json=True)

# GOOD: внутри callable
def my_task(**context):
    config = Variable.get("my_config", deserialize_json=True)
    ...

# GOOD: через Jinja-шаблон
BashOperator(
    task_id="run",
    bash_command="echo {{ var.value.my_config }}"
)
```

---

## 3. Атомарность задач (Task Atomicity)

**Что это:** Каждая задача выполняет одну операцию и может быть безопасно перезапущена.

**Правило ETL:** Extract, Transform, Load — три отдельные задачи.

**Антипаттерн:**
```python
# BAD: всё в одной задаче
def extract_transform_load():
    data = fetch_from_api()      # Extract
    data = clean(data)           # Transform
    load_to_db(data)             # Load
```

**Хороший паттерн:**
```python
# GOOD: атомарные задачи
@task
def extract() -> list:
    return fetch_from_api()

@task
def transform(raw: list) -> list:
    return clean(raw)

@task
def load(clean_data: list) -> None:
    load_to_db(clean_data)
```

---

## 4. Ретраи (Retries)

**Рекомендуемые значения:**
```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,   # экспоненциальный backoff
    "max_retry_delay": timedelta(hours=1),
}
```

**Для внешних API с rate limiting:**
```python
"retry_delay": timedelta(minutes=1),
"retry_exponential_backoff": True,
```

---

## 5. XCom — когда и как

**XCom подходит для:**
- Небольших метаданных (IDs, статусы, пути к файлам)
- Строк и числовых значений
- Небольших словарей (< 48 KB рекомендуется)

**XCom НЕ подходит для:**
- DataFrame / больших массивов данных
- Бинарных данных
- Всего, что > ~1 MB

**Правильный паттерн для данных:**
```python
@task
def extract() -> str:
    data = fetch_large_dataset()
    # Сохрани данные во внешнее хранилище
    path = f"s3://bucket/tmp/run_{context['run_id']}.parquet"
    data.to_parquet(path)
    return path  # Передай только путь через XCom

@task
def transform(data_path: str) -> str:
    data = pd.read_parquet(data_path)
    ...
```

---

## 6. TaskFlow API vs традиционные операторы

**TaskFlow API (рекомендуется для Python):**
```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def my_dag():
    @task
    def extract() -> dict:
        return {"key": "value"}

    @task
    def transform(data: dict) -> dict:
        return {k: v.upper() for k, v in data.items()}

    raw = extract()
    transform(raw)

my_dag()
```

**Преимущества TaskFlow:**
- Автоматическая передача данных через XCom
- Граф строится автоматически из зависимостей функций
- Чище и читаемее
- Легче тестировать (просто вызови функцию)

---

## 7. Dynamic Task Mapping (Airflow 2.3+)

**Используй вместо статических fan-out паттернов:**
```python
# BAD: статический fan-out
for i in range(10):
    PythonOperator(task_id=f"process_{i}", ...)

# GOOD: Dynamic Task Mapping
@task
def get_items() -> list:
    return fetch_items_from_db()

@task
def process_item(item: dict) -> None:
    ...

process_item.expand(item=get_items())
```

---

## 8. Catchup и backfill

**`catchup=True` (по умолчанию):** При запуске DAG с исторической `start_date` создаёт DAG Run для каждого пропущенного интервала.

**Когда `catchup=False`:**
- Реальное время: мониторинг, дашборды
- Пайплайны, которые не имеет смысла запускать ретроспективно

**Когда `catchup=True`:**
- ETL с историческими данными
- Когда важен каждый период

**Явное указание — всегда лучшая практика:**
```python
DAG(
    "my_dag",
    catchup=False,  # Явно, даже если False — по умолчанию
    ...
)
```

---

## 9. Безопасность (Secrets)

**Порядок приоритетов для секретов:**

1. **Airflow Connections** — лучший выбор для credentials к сервисам
2. **Secrets Backend** (Vault, AWS SM, GCP SM) — для prod-окружений
3. **Airflow Variables с шифрованием** — для конфигурационных значений
4. **Environment variables** — простой вариант для single-node

**Никогда:** хардкоженные значения в коде или `airflow.cfg`.

---

## 10. Документирование DAG

```python
"""
ETL pipeline для загрузки данных о продажах из CRM в Data Warehouse.

Расписание: ежедневно в 03:00 UTC
Владелец: data-engineering@company.com
Зависимости: CRM API, Snowflake DWH
SLA: данные должны быть доступны к 06:00 UTC

Граф:
    extract_sales → validate → transform → load_to_dw → notify
"""
from airflow.decorators import dag

@dag(
    doc_md=__doc__,
    ...
)
def sales_etl():
    ...
```

---

## 11. Sensor modes и deferrable operators

**Проблема:** по умолчанию `mode='poke'` держит worker-слот занятым весь период ожидания — сенсор буквально блокирует воркер, пока не дождётся условия.

**Приоритет решений (от худшего к лучшему):**
```python
# ХУДШИЙ: mode='poke' (по умолчанию) на долгое ожидание
S3KeySensor(
    task_id="wait_for_file",
    bucket_key="data/{{ ds }}/input.csv",
    poke_interval=300,
    timeout=7200,
    # mode не указан → poke → воркер занят все 2 часа
)

# ЛУЧШЕ: mode='reschedule' — освобождает воркер между проверками
S3KeySensor(
    task_id="wait_for_file",
    bucket_key="data/{{ ds }}/input.csv",
    mode="reschedule",
    poke_interval=300,
    timeout=7200,
)

# ЛУЧШЕ ВСЕГО: deferrable=True (Airflow 2.2+, нужен triggerer) —
# задача передаётся triggerer'у, worker-слот освобождается полностью
S3KeySensor(
    task_id="wait_for_file",
    bucket_key="data/{{ ds }}/input.csv",
    deferrable=True,
)
```

**Когда `mode='poke'` уместен:** только для очень коротких ожиданий (секунды/единицы минут), где накладные расходы на reschedule/deferral не окупаются.

---

## 12. Устаревшие паттерны

**SubDagOperator** — anti-pattern с Airflow 2.0: сабдаг сам занимает worker-слот, ожидая слоты для своих внутренних задач, что может привести к deadlock планировщика при небольшом пуле воркеров.
```python
# BAD
from airflow.operators.subdag import SubDagOperator

# GOOD — используй TaskGroup
from airflow.decorators import task_group

@task_group
def my_group():
    ...
```

**`execution_date` в контексте** — deprecated с Airflow 2.2. Для DAG с нестандартным расписанием (data-driven, кастомные timetables) это поле теряет однозначный смысл.
```python
# BAD
@task
def process(**context):
    execution_date = context["execution_date"]

# GOOD
@task
def process(**context):
    logical_date = context["logical_date"]
    start = context["data_interval_start"]
```

**Прямой доступ к metadata DB** — обращение к `DagModel`/`TaskInstance`/`DagRun` через `airflow.settings.Session` не является публичным API: схема меняется между минорными версиями Airflow, а лишние соединения нагружают БД метаданных, от которой зависит весь scheduler.
```python
# BAD
from airflow.settings import Session
session = Session()
session.query(TaskInstance).filter(...)

# GOOD — используй Airflow REST API или CLI
# GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances
```

---

## 13. Мониторинг и алерты

**Callback-паттерн (рекомендуется):**
```python
def slack_alert(context):
    """Отправляет уведомление в Slack при сбое."""
    task_instance = context.get('task_instance')
    SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_default',
        message=f"DAG {task_instance.dag_id} провалился на {task_instance.task_id}",
    ).execute(context)

default_args = {
    "on_failure_callback": slack_alert,
}
```

**SLA Misses:**
```python
DAG(
    "critical_dag",
    sla_miss_callback=my_sla_callback,
    default_args={"sla": timedelta(hours=2)},
)
```

---

## 14. Группировка тасков и зависимостей

**Что это:** создание тасков/операторов и объявление зависимостей (`>>`, `set_downstream`, TaskFlow-вызовы, `.expand`/`.partial`) собраны в одном месте — как правило, одним блоком в конце DAG, после вспомогательных функций и конфигурации.

**Почему важно:** DAG — это в первую очередь граф. Когда инстанцирование тасков перемежается с другим кодом (helper-функциями, условиями, конфигурацией), граф перестаёт читаться целиком — приходится листать файл вверх-вниз, чтобы понять реальный порядок выполнения.

**Антипаттерн:**
```python
# BAD: таски и зависимости разбросаны по файлу
with DAG("etl", ...) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    THRESHOLD = 100  # конфигурация затесалась между тасками

    def _validate(**context):
        ...

    validate_task = PythonOperator(task_id="validate", python_callable=_validate)
    extract_task >> validate_task  # зависимость объявлена не в конце, а сразу здесь

    load_task = PythonOperator(task_id="load", python_callable=load)
    validate_task >> load_task
```

**Хороший паттерн:**
```python
# GOOD: сначала вся конфигурация и функции, потом — единый блок тасков и зависимостей
with DAG("etl", ...) as dag:
    THRESHOLD = 100

    def _validate(**context):
        ...

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    validate_task = PythonOperator(task_id="validate", python_callable=_validate)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> validate_task >> load_task
```

Это же правило действует и для TaskFlow API — вызовы `extract()`, `transform()`, `load()` и их цепочка должны идти одним блоком в конце функции DAG.
