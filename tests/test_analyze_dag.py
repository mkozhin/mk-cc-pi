#!/usr/bin/env python3
"""
Smoke-тест для plugins/mk-airflow/skills/dag-review/scripts/analyze_dag.py.
Не тянет pytest — просто набор assert'ов, падает с ненулевым кодом при провале.

Запуск: python3 tests/test_analyze_dag.py
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "plugins/mk-airflow/skills/dag-review/scripts/analyze_dag.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def run(path: Path, *extra_args: str) -> tuple[int, dict | None, str]:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--path", str(path), *extra_args],
        capture_output=True,
        text=True,
    )
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        data = None
    return result.returncode, data, result.stdout


def test_good_dag():
    code, data, _ = run(FIXTURES / "good_dag.py")
    assert code == 0, "скрипт должен завершаться успешно на валидном DAG"
    assert data is not None, "вывод должен быть валидным JSON"
    assert data["syntax_ok"] is True
    assert data["dag_found"] is True
    assert data["score"] >= 85, f"хорошо оформленный DAG должен получать высокую оценку, получили {data['score']}"


def test_bad_dag_syntax_error():
    code, data, _ = run(FIXTURES / "bad_dag.py")
    assert data is not None, "вывод должен быть валидным JSON даже при синтаксической ошибке"
    assert data["syntax_ok"] is False
    assert any(i["severity"] == "critical" for i in data["issues"])


def test_no_dag_found():
    code, data, _ = run(FIXTURES / "no_dag.py")
    assert data is not None
    assert data["syntax_ok"] is True
    assert data["dag_found"] is False
    assert any(i["title"] == "DAG-объект не найден" for i in data["issues"])


def test_missing_file():
    code, data, _ = run(FIXTURES / "does_not_exist.py")
    assert code != 0, "скрипт должен завершаться с ошибкой, если файл не найден"
    assert data is not None
    assert "error" in data


def test_antipatterns_detected():
    code, data, _ = run(FIXTURES / "antipatterns_dag.py")
    assert data is not None
    titles = [i["title"] for i in data["issues"]]

    assert any("Sensor" in t and "deferrable" in t for t in titles), titles
    assert any("SubDagOperator" in t for t in titles), titles
    assert any("execution_date" in t for t in titles), titles
    assert any("metadata DB" in t for t in titles), titles


def test_deferrable_sensor_is_ok_not_warning():
    code, data, _ = run(FIXTURES / "sensor_deferrable_dag.py")
    assert data is not None
    sensor_issues = [i for i in data["issues"] if "Sensor" in i["title"]]
    assert len(sensor_issues) == 1
    assert sensor_issues[0]["severity"] == "ok"


def test_scattered_tasks_detected():
    code, data, _ = run(FIXTURES / "scattered_tasks_dag.py")
    assert data is not None
    assert any(i["title"] == "Вызовы тасков/операторов разбросаны по DAG" for i in data["issues"])


def test_grouped_tasks_not_flagged():
    code, data, _ = run(FIXTURES / "grouped_tasks_dag.py")
    assert data is not None
    assert not any(i["title"] == "Вызовы тасков/операторов разбросаны по DAG" for i in data["issues"])


def test_pretty_flag_matches_docstring():
    docstring = SCRIPT.read_text(encoding="utf-8")
    assert "--pretty" in docstring, "usage-докстринг должен ссылаться на реально существующий флаг --pretty"
    code, _, stdout = run(FIXTURES / "good_dag.py", "--pretty")
    assert code == 0
    assert "\n" in stdout.strip(), "--pretty должен давать многострочный (indent=2) JSON"


TESTS = [
    test_good_dag,
    test_bad_dag_syntax_error,
    test_no_dag_found,
    test_missing_file,
    test_antipatterns_detected,
    test_deferrable_sensor_is_ok_not_warning,
    test_scattered_tasks_detected,
    test_grouped_tasks_not_flagged,
    test_pretty_flag_matches_docstring,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except AssertionError as e:
            failures += 1
            print(f"FAIL {name}: {e}")
        else:
            print(f"OK   {name}")
    if failures:
        print(f"\n{failures}/{len(TESTS)} тестов упало")
        return 1
    print(f"\nвсе {len(TESTS)} тестов прошли")
    return 0


if __name__ == "__main__":
    sys.exit(main())
