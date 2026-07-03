#!/usr/bin/env python3
"""
Airflow DAG static analyzer.
Outputs JSON for consumption by the Claude skill.

Usage:
    python3 analyze_dag.py --path /path/to/dag.py
    python3 analyze_dag.py --path /path/to/dag.py --pretty   # pretty-print
"""

import ast
import json
import re
import sys
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

class Issue:
    def __init__(self, severity: str, category: str, title: str, detail: str, fix: str = ""):
        self.severity = severity   # critical | warning | info | ok
        self.category = category
        self.title = title
        self.detail = detail
        self.fix = fix

    def to_dict(self):
        return {
            "severity": self.severity,
            "category": self.category,
            "title": self.title,
            "detail": self.detail,
            "fix": self.fix,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Analyzer
# ─────────────────────────────────────────────────────────────────────────────

class DAGAnalyzer:
    def __init__(self, code: str):
        self.code = code
        self.issues: list[Issue] = []
        self.tree: ast.Module | None = None
        self.dag_calls: list[ast.Call] = []
        self.score = 100
        self.syntax_ok = False
        self.dag_found = False

    def analyze(self) -> dict:
        self._check_syntax()
        if self.tree:
            self._find_dag_calls()
            self._check_top_level_code()
            self._check_dag_params()
            self._check_schedule()
            self._check_catchup()
            self._check_tags()
            self._check_secrets()
            self._check_idempotency()
            self._check_xcom()
            self._check_imports()
            self._check_taskflow()
            self._check_dynamic_mapping()
            self._check_documentation()
            self._check_python_callable_patterns()
            self._check_task_atomicity()
            self._check_dependency_style()
            self._check_logging()
            self._check_on_failure_callback()
            self._check_sensors()
            self._check_subdag()
            self._check_execution_date()
            self._check_metadata_db_access()
            self._check_task_grouping()

        score = max(0, min(100, self.score))
        if score >= 85:
            grade = "Отлично"
        elif score >= 70:
            grade = "Хорошо"
        elif score >= 50:
            grade = "Требует улучшений"
        else:
            grade = "Критические проблемы"

        return {
            "score": score,
            "grade": grade,
            "syntax_ok": self.syntax_ok,
            "dag_found": self.dag_found,
            "issues": [i.to_dict() for i in self.issues],
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _add(self, severity, category, title, detail, fix="", penalty=0):
        self.issues.append(Issue(severity, category, title, detail, fix))
        self.score -= penalty

    def _has_kw(self, dag_call: ast.Call, key: str) -> bool:
        """Check if DAG call has keyword arg or it's in default_args dict/var."""
        for kw in dag_call.keywords:
            if kw.arg == key:
                return True
        return self._default_args_has_key(dag_call, key)

    def _default_args_has_key(self, dag_call: ast.Call, key: str) -> bool:
        for kw in dag_call.keywords:
            if kw.arg == "default_args":
                val = kw.value
                if isinstance(val, ast.Dict):
                    for k in val.keys:
                        if isinstance(k, ast.Constant) and k.value == key:
                            return True
                elif isinstance(val, ast.Name):
                    varname = val.id
                    pattern = rf'{re.escape(varname)}\s*=\s*\{{[^}}]*["\']?{re.escape(key)}["\']?\s*:'
                    if re.search(pattern, self.code):
                        return True
        return False

    # ── Checks ────────────────────────────────────────────────────────────────

    def _check_syntax(self):
        try:
            self.tree = ast.parse(self.code)
            self.syntax_ok = True
        except SyntaxError as e:
            self._add(
                "critical", "Синтаксис",
                "Синтаксическая ошибка",
                f"Строка {e.lineno}: {e.msg}",
                "Исправьте синтаксическую ошибку — DAG не загрузится в Airflow.",
                penalty=50,
            )

    def _find_dag_calls(self):
        for node in ast.walk(self.tree):
            # with DAG(...) / dag = DAG(...)
            if isinstance(node, ast.Call):
                name = ""
                if isinstance(node.func, ast.Attribute):
                    name = node.func.attr
                elif isinstance(node.func, ast.Name):
                    name = node.func.id
                if name == "DAG":
                    self.dag_calls.append(node)
            # @dag(...) decorator
            if isinstance(node, ast.FunctionDef):
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call):
                        dec_name = ""
                        if isinstance(dec.func, ast.Name):
                            dec_name = dec.func.id
                        elif isinstance(dec.func, ast.Attribute):
                            dec_name = dec.func.attr
                        if dec_name == "dag":
                            self.dag_calls.append(dec)
                    elif isinstance(dec, ast.Name) and dec.id == "dag":
                        # @dag без скобок — создаём пустой Call-stub
                        stub = ast.Call(func=dec, args=[], keywords=[])
                        self.dag_calls.append(stub)
        self.dag_found = bool(self.dag_calls)

    # ── Группировка тасков ───────────────────────────────────────────────────

    @staticmethod
    def _callable_name(node) -> str:
        """Имя вызываемого объекта для Call/декоратора: Attribute.attr или Name.id."""
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Name):
            return node.id
        return ""

    def _dag_body_blocks(self) -> list:
        """Тела `with DAG(...) as dag:` и функций, декорированных `@dag`."""
        blocks = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.With):
                for item in node.items:
                    call = item.context_expr
                    if isinstance(call, ast.Call) and self._callable_name(call.func) == "DAG":
                        blocks.append(node.body)
            elif isinstance(node, ast.FunctionDef):
                for dec in node.decorator_list:
                    dec_call = dec.func if isinstance(dec, ast.Call) else dec
                    if self._callable_name(dec_call) == "dag":
                        blocks.append(node.body)
        return blocks

    def _taskflow_function_names(self) -> set:
        """Имена функций, декорированных `@task` (включая `@task(...)`)."""
        names = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for dec in node.decorator_list:
                dec_call = dec.func if isinstance(dec, ast.Call) else dec
                if self._callable_name(dec_call) == "task":
                    names.add(node.name)
        return names

    def _is_task_related_stmt(self, stmt, taskflow_names: set) -> bool:
        """Инстанцирование оператора/сенсора, TaskFlow-вызов или связывание зависимостей."""
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.BinOp) \
                and isinstance(stmt.value.op, (ast.RShift, ast.LShift)):
            return True  # a >> b / a << b

        call = None
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
            call = stmt.value
        elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            call = stmt.value
        if call is None:
            return False

        name = self._callable_name(call.func)
        if name.endswith("Operator") or name.endswith("Sensor"):
            return True
        if name in ("set_downstream", "set_upstream", "expand", "partial"):
            return True
        return any(isinstance(n, ast.Name) and n.id in taskflow_names for n in ast.walk(call))

    def _check_task_grouping(self):
        taskflow_names = self._taskflow_function_names()
        for body in self._dag_body_blocks():
            related_idx = [
                i for i, stmt in enumerate(body)
                if self._is_task_related_stmt(stmt, taskflow_names)
            ]
            if len(related_idx) < 2:
                continue
            first, last = related_idx[0], related_idx[-1]
            related_set = set(related_idx)
            if any(i not in related_set for i in range(first, last + 1)):
                self._add(
                    "warning", "Читаемость",
                    "Вызовы тасков/операторов разбросаны по DAG",
                    "Создание тасков и связывание зависимостей (`>>`, `set_downstream`, "
                    "TaskFlow-вызовы, `.expand`/`.partial`) перемежаются с другим кодом внутри "
                    "DAG-блока. Из-за этого сложно охватить взглядом весь граф выполнения — "
                    "приходится листать файл вверх-вниз, чтобы понять порядок задач.",
                    "Собери создание тасков и объявление зависимостей в одном месте — "
                    "как правило, внизу DAG-блока, после вспомогательных функций и конфигурации.",
                    penalty=6,
                )
                break

    def _check_top_level_code(self):
        patterns = [
            (r"\bVariable\.get\b", "Variable.get()"),
            (r"\bConnection\.get\b", "Connection.get()"),
            (r"\brequests\.(get|post|put|delete|patch)\b", "HTTP-запрос (requests)"),
            (r"\bopen\s*\([^)]+[\"']r[\"']", "Чтение файла (open)"),
            (r"\bpsycopg2\b|\bpymysql\b|\bsqlite3\.connect\b", "Прямое подключение к БД"),
            (r"\bsmtplib\b", "SMTP-соединение"),
        ]
        for pattern, label in patterns:
            if re.search(pattern, self.code):
                self._add(
                    "critical", "Top-level код",
                    f"{label} на уровне модуля",
                    "Код на уровне модуля выполняется при каждом парсинге DAG-файла "
                    "планировщиком (~каждые 30 с). Замедляет scheduler, создаёт утечки ресурсов, "
                    "делает DAG-файл хрупким (упадёт если сервис недоступен).",
                    "Перенеси в callable задачи или используй Jinja-шаблоны / Connections.",
                    penalty=15,
                )

    def _check_dag_params(self):
        if not self.dag_calls:
            self._add(
                "critical", "Структура DAG",
                "DAG-объект не найден",
                "Файл не содержит вызова DAG(). Airflow не распознает его как DAG-файл.",
                "Определи DAG через `with DAG(...) as dag:` или декоратор `@dag`.",
                penalty=30,
            )
            return

        for dag_call in self.dag_calls:
            # retries
            if not self._has_kw(dag_call, "retries"):
                self._add(
                    "warning", "Надёжность",
                    "Нет параметра `retries`",
                    "Без ретраев любой transient-сбой (сетевой таймаут, OOM, "
                    "временная недоступность внешнего сервиса) завершит задачу неудачей. "
                    "В распределённых средах это случается регулярно.",
                    "Добавь `retries=2` в `default_args` или напрямую в DAG.",
                    penalty=8,
                )

            # retry_delay
            if self._has_kw(dag_call, "retries") and not self._has_kw(dag_call, "retry_delay"):
                self._add(
                    "info", "Надёжность",
                    "Нет явного `retry_delay`",
                    "По умолчанию задержка между ретраями — 5 минут. "
                    "Для быстрых задач это избыточно, для внешних API с rate limiting — недостаточно.",
                    "Добавь явно: `retry_delay=timedelta(minutes=5)` в `default_args`.",
                )

            # owner
            if not self._has_kw(dag_call, "owner"):
                self._add(
                    "warning", "Метаданные",
                    "Нет `owner`",
                    "Без owner невозможно понять кто отвечает за DAG. "
                    "Критично при алертах — кому писать?",
                    "Добавь `owner='team-name'` в `default_args`.",
                    penalty=5,
                )

            # start_date
            if not self._has_kw(dag_call, "start_date"):
                self._add(
                    "critical", "Структура DAG",
                    "Нет `start_date`",
                    "DAG без `start_date` не будет запланирован Airflow.",
                    "Добавь `start_date=datetime(2024, 1, 1)` в `default_args`.",
                    penalty=20,
                )

            # email / on_failure_callback (monitored separately)
            has_email_fail = self._has_kw(dag_call, "email_on_failure")
            has_callback = "on_failure_callback" in self.code
            if not has_email_fail and not has_callback:
                self._add(
                    "info", "Мониторинг",
                    "Нет оповещений о сбоях",
                    "Без `email_on_failure` или `on_failure_callback` команда не узнает о падениях.",
                    "Добавь `email_on_failure=True` + `email=[...]` или настрой `on_failure_callback`.",
                )

            # execution_timeout
            if not self._has_kw(dag_call, "execution_timeout") and \
               "execution_timeout" not in self.code:
                self._add(
                    "info", "Надёжность",
                    "Нет `execution_timeout`",
                    "Задачи без таймаута могут зависнуть навсегда и блокировать worker-слоты.",
                    "Добавь `execution_timeout=timedelta(hours=1)` (или подходящее значение) в `default_args`.",
                )

    def _check_schedule(self):
        if re.search(r'\bschedule_interval\b', self.code):
            self._add(
                "warning", "Совместимость",
                "Устаревший параметр `schedule_interval`",
                "В Airflow 2.4+ параметр `schedule_interval` устарел. "
                "В Airflow 3.x он будет удалён.",
                "Замени на `schedule='...'`.",
                penalty=5,
            )

        # cron without comment
        cron_match = re.search(r'schedule\s*=\s*["\']([^"\'@\s][^"\']*)["\']', self.code)
        if cron_match:
            cron = cron_match.group(1)
            if re.match(r'^[\d\*/,\-\s]+$', cron):
                line_start = self.code.rfind('\n', 0, cron_match.start()) + 1
                line_end = self.code.find('\n', cron_match.end())
                line_text = self.code[line_start:line_end if line_end != -1 else len(self.code)]
                if '#' not in line_text:
                    self._add(
                        "info", "Читаемость",
                        f"Cron `{cron}` без комментария",
                        "Нетривиальное cron-выражение трудно читать без пояснения.",
                        f"Добавь комментарий: `schedule='{cron}'  # каждый день в 3:00 UTC`",
                    )

    def _check_catchup(self):
        if "catchup" not in self.code:
            self._add(
                "warning", "Поведение DAG",
                "Параметр `catchup` не задан явно",
                "По умолчанию `catchup=True`. При первом запуске с исторической `start_date` "
                "Airflow создаст DAG Run для каждого пропущенного интервала — "
                "это может породить тысячи задач и перегрузить кластер.",
                "Добавь `catchup=False` если backfill не нужен. "
                "Если нужен — задай явно `catchup=True` (для ясности намерения).",
                penalty=10,
            )

    def _check_tags(self):
        if "tags" not in self.code:
            self._add(
                "info", "Метаданные",
                "Нет `tags`",
                "Теги помогают фильтровать и организовывать DAGи в UI по командам и доменам.",
                "Добавь `tags=['team', 'domain', 'prod']` в DAG.",
                penalty=3,
            )

    def _check_secrets(self):
        patterns = [
            (r'(?i)password\s*=\s*["\'][^"\']{3,}["\']', "пароль"),
            (r'(?i)secret\s*=\s*["\'][^"\']{3,}["\']', "секрет"),
            (r'(?i)api[_-]?key\s*=\s*["\'][^"\']{8,}["\']', "API-ключ"),
            (r'(?i)token\s*=\s*["\'][A-Za-z0-9_\-]{16,}["\']', "токен"),
            (r'AKIA[A-Z0-9]{16}', "AWS Access Key ID"),
            (r'(?i)private[_-]?key\s*=\s*["\']-----BEGIN', "приватный ключ"),
        ]
        for pattern, label in patterns:
            if re.search(pattern, self.code):
                self._add(
                    "critical", "Безопасность",
                    f"Возможный хардкоженный секрет: {label}",
                    f"Хранение {label}а прямо в DAG-файле критически опасно. "
                    "DAG-файлы хранятся в git и доступны всем кто видит репозиторий.",
                    "Используй Airflow Connections, переменные с шифрованием (Variables), "
                    "или внешние хранилища: HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager.",
                    penalty=25,
                )

    def _check_idempotency(self):
        lines = self.code.split('\n')
        for i, line in enumerate(lines, 1):
            if re.search(r'datetime\.(now|today)\(\)', line):
                indent = len(line) - len(line.lstrip())
                if indent < 4:
                    self._add(
                        "warning", "Идемпотентность",
                        f"Строка {i}: `datetime.now/today()` на уровне модуля",
                        "DAG становится неидемпотентным: при ретрае за прошедшую дату "
                        "`datetime.now()` вернёт текущее время, а не время исходного запуска. "
                        "Данные будут некорректны.",
                        "Используй Jinja: `{{ ds }}` (дата), `{{ data_interval_start }}`, "
                        "`{{ prev_start_date_success }}`.",
                        penalty=10,
                    )
                    break

        hardcoded = re.findall(r'["\']20\d{2}-\d{2}-\d{2}["\']', self.code)
        if len(hardcoded) > 2:
            self._add(
                "warning", "Идемпотентность",
                f"Найдено {len(hardcoded)} захардкоженных дат в логике",
                "Жёстко заданные даты в условиях задач нарушают идемпотентность "
                "и делают невозможным корректный backfill.",
                "Замени на Jinja-шаблоны или параметры DAG Run.",
                penalty=8,
            )

    def _check_xcom(self):
        if not re.search(r'xcom_push|xcom_pull|\.output\b', self.code):
            return
        if re.search(r'(?i)xcom_push.*?(df|dataframe|list\s*=|dict\s*=)', self.code):
            self._add(
                "warning", "Производительность",
                "Возможна передача больших данных через XCom",
                "XCom хранится в БД метаданных Airflow. DataFrame или большие списки "
                "могут перегрузить БД и замедлить scheduler для всего кластера.",
                "Передавай через XCom только идентификаторы/пути. "
                "Сами данные — через S3, GCS, HDFS или другое внешнее хранилище.",
                penalty=10,
            )

    def _check_imports(self):
        deprecated_map = [
            ("airflow.operators.python_operator", "airflow.operators.python", "PythonOperator"),
            ("airflow.operators.bash_operator",   "airflow.operators.bash",   "BashOperator"),
            ("airflow.operators.dummy_operator",   "airflow.operators.empty",  "EmptyOperator"),
            ("airflow.operators.dummy",            "airflow.operators.empty",  "EmptyOperator"),
            ("airflow.sensors.base_sensor_operator", "airflow.sensors.base",   "BaseSensorOperator"),
            ("airflow.contrib.",                   "apache-airflow-providers-*", "(provider-package)"),
        ]
        for old_mod, new_mod, cls in deprecated_map:
            if old_mod in self.code:
                self._add(
                    "warning", "Совместимость",
                    f"Устаревший модуль `{old_mod}`",
                    f"Этот модуль удалён или помечен устаревшим в Airflow 2.x / 3.x.",
                    f"Замени на: `from {new_mod} import {cls}`",
                    penalty=5,
                )

    def _check_taskflow(self):
        if "@task" in self.code or "from airflow.decorators import" in self.code:
            self._add(
                "ok", "TaskFlow API",
                "Используется TaskFlow API",
                "TaskFlow API (@task) — рекомендуемый способ для Python-задач в Airflow 2.x. "
                "Упрощает передачу данных между задачами и делает граф более читаемым.",
            )

    def _check_dynamic_mapping(self):
        if ".expand(" in self.code or ".partial(" in self.code:
            self._add(
                "ok", "Dynamic Task Mapping",
                "Используется Dynamic Task Mapping",
                "Dynamic Task Mapping (.expand/.partial) — правильный способ "
                "параллельной обработки переменного числа элементов (Airflow 2.3+). "
                "Избегает создания статических 'fan-out' DAGов.",
            )

    def _check_documentation(self):
        has_doc_md = "doc_md" in self.code
        has_module_docstring = (
            self.tree and
            self.tree.body and
            isinstance(self.tree.body[0], ast.Expr) and
            isinstance(self.tree.body[0].value, ast.Constant) and
            isinstance(self.tree.body[0].value.value, str) and
            len(self.tree.body[0].value.value) > 30
        )
        if not has_doc_md and not has_module_docstring:
            self._add(
                "info", "Документация",
                "DAG не документирован",
                "Без документации новые члены команды не поймут назначение DAGа, "
                "его входные данные и ожидаемые результаты.",
                "Добавь `doc_md=__doc__` в DAG + модульный docstring с описанием. "
                "Текст отображается прямо в Airflow UI.",
                penalty=3,
            )

    def _check_python_callable_patterns(self):
        if re.search(r'python_callable\s*=\s*lambda', self.code):
            self._add(
                "warning", "Лучшие практики",
                "Lambda в `python_callable`",
                "Lambda-функции плохо сериализуются Airflow, не имеют имени в логах "
                "и затрудняют профилирование и отладку.",
                "Используй именованную функцию вместо lambda.",
                penalty=7,
            )

        if "provide_context=True" in self.code:
            self._add(
                "warning", "Совместимость",
                "Устаревший `provide_context=True`",
                "В Airflow 2.x контекст передаётся автоматически. "
                "`provide_context` игнорируется, но сигнализирует о Airflow 1.x-коде.",
                "Удали `provide_context=True`.",
                penalty=5,
            )

    def _check_task_atomicity(self):
        bash_cmds = re.findall(r'bash_command\s*=\s*(?:f?["\'])((?:[^"\'\\]|\\.)+)', self.code)
        for cmd in bash_cmds:
            ops = cmd.count('&&') + cmd.count(';') + cmd.count('|')
            if ops >= 3:
                self._add(
                    "warning", "Атомарность",
                    f"Длинная цепочка в BashOperator ({ops} операций)",
                    "Нарушает атомарность: при сбое непонятно на каком шаге упало, "
                    "и ретрай может выполнить уже выполненные шаги повторно.",
                    "Разбей на отдельные BashOperator задачи или используй PythonOperator.",
                    penalty=7,
                )

    def _check_dependency_style(self):
        has_bitshift  = ">>" in self.code or "<<" in self.code
        has_set_style = "set_downstream" in self.code or "set_upstream" in self.code
        if has_bitshift and has_set_style:
            self._add(
                "warning", "Читаемость",
                "Смешаны стили зависимостей (`>>` и `set_downstream`)",
                "Разные стили задания зависимостей в одном файле усложняют понимание графа.",
                "Используй единый стиль — предпочтительно `>>` как более читаемый.",
                penalty=5,
            )

    def _check_logging(self):
        lines = self.code.split('\n')
        print_lines = [
            i for i, l in enumerate(lines, 1)
            if re.match(r'\s*print\s*\(', l) and not l.lstrip().startswith('#')
        ]
        if print_lines:
            sample = ', '.join(map(str, print_lines[:5]))
            suffix = '...' if len(print_lines) > 5 else ''
            self._add(
                "info", "Лучшие практики",
                f"print() вместо logging (строки: {sample}{suffix})",
                "Вывод через print() не попадает в структурированные логи Airflow "
                "и теряется при параллельном выполнении в распределённой среде.",
                "Используй: `import logging; log = logging.getLogger(__name__)` "
                "затем `log.info(...)`, `log.warning(...)`, `log.error(...)`.",
                penalty=3,
            )

    def _check_on_failure_callback(self):
        # Positive check — mention if they have a good callback
        if "on_failure_callback" in self.code:
            self._add(
                "ok", "Мониторинг",
                "Настроен `on_failure_callback`",
                "Отлично! Callback при сбое — правильный способ оповещений "
                "(гибче чем email_on_failure).",
            )

    def _check_sensors(self):
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call):
                continue
            name = ""
            if isinstance(node.func, ast.Attribute):
                name = node.func.attr
            elif isinstance(node.func, ast.Name):
                name = node.func.id
            if not name.endswith("Sensor"):
                continue

            deferrable = False
            mode_reschedule = False
            for kw in node.keywords:
                if kw.arg == "deferrable" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
                    deferrable = True
                if kw.arg == "mode" and isinstance(kw.value, ast.Constant) and kw.value.value == "reschedule":
                    mode_reschedule = True

            if deferrable:
                self._add(
                    "ok", "Производительность",
                    f"`{name}` использует `deferrable=True`",
                    "Deferrable-режим освобождает worker-слот на время ожидания "
                    "(задача передаётся triggerer'у) — самый эффективный вариант для сенсоров.",
                )
            elif not mode_reschedule:
                self._add(
                    "warning", "Производительность",
                    f"`{name}` без `deferrable=True` или `mode='reschedule'`",
                    "По умолчанию `mode='poke'` держит worker-слот занятым на всё время ожидания. "
                    "При долгих ожиданиях (минуты/часы) это забивает пул воркеров впустую.",
                    "Добавь `deferrable=True` (Airflow 2.2+, требует triggerer) "
                    "или как минимум `mode='reschedule'`.",
                    penalty=8,
                )

    def _check_subdag(self):
        if "SubDagOperator" in self.code or "airflow.operators.subdag" in self.code:
            self._add(
                "warning", "Совместимость",
                "Используется устаревший `SubDagOperator`",
                "SubDAG считается anti-pattern начиная с Airflow 2.0: провоцирует deadlock "
                "планировщика (сабдаг сам занимает worker-слот, ожидая слоты для своих задач) "
                "и плохо масштабируется.",
                "Замени на TaskGroup: `from airflow.decorators import task_group` "
                "(или `from airflow.utils.task_group import TaskGroup` для классического API).",
                penalty=10,
            )

    def _check_execution_date(self):
        if re.search(r'\bexecution_date\b', self.code):
            self._add(
                "info", "Совместимость",
                "Используется устаревший контекст `execution_date`",
                "С Airflow 2.2 `execution_date` считается deprecated: для DAG с не-cron "
                "расписанием (data-driven триггеры, кастомные timetables) это поле "
                "теряет однозначный смысл.",
                "Замени на `logical_date` (прямой аналог) или на `data_interval_start`/"
                "`data_interval_end`, если нужна именно граница интервала.",
                penalty=3,
            )

    def _check_metadata_db_access(self):
        patterns = [
            r'from\s+airflow\.settings\s+import\s+Session\b',
            r'from\s+airflow\.models\s+import[^\n]*\b(?:DagModel|TaskInstance|DagRun)\b',
            r'\bcreate_session\s*\(',
        ]
        if any(re.search(p, self.code) for p in patterns):
            self._add(
                "warning", "Совместимость",
                "Прямой доступ к metadata DB Airflow",
                "Прямые запросы к внутренним таблицам (`DagModel`, `TaskInstance`, `DagRun`) "
                "через ORM-сессию Airflow — не публичный API: схема меняется между минорными "
                "версиями, а лишние соединения нагружают БД метаданных, от которой зависит "
                "весь scheduler.",
                "Используй Airflow REST API (`/api/v1/...`) или CLI вместо прямых запросов "
                "к metadata DB.",
                penalty=10,
            )


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Airflow DAG static analyzer")
    parser.add_argument("--path", required=True, help="Path to DAG Python file")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        result = {"error": f"File not found: {path}", "score": 0, "issues": []}
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(1)

    code = path.read_text(encoding="utf-8")
    analyzer = DAGAnalyzer(code)
    result = analyzer.analyze()

    indent = 2 if args.pretty else None
    print(json.dumps(result, ensure_ascii=False, indent=indent))


if __name__ == "__main__":
    main()
