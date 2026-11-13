"""Make the DAG modules importable in a plain Python environment (laptop / CI).

Inside the Airflow image the real `airflow` package exists and is used. Anywhere
else (no Airflow at all, or Airflow installed without the Postgres provider) the
missing Airflow modules are replaced by harmless stand-ins, one module at a time,
so the business logic in dags/ can be tested without running Airflow.
"""
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

# Parents first, so a missing parent is stubbed before its children.
AIRFLOW_MODULES = (
    "airflow",
    "airflow.sdk",
    "airflow.exceptions",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
)


def _importable(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:  # missing module, or a broken partial install
        return False


for _name in AIRFLOW_MODULES:
    if not _importable(_name):
        sys.modules[_name] = MagicMock()
        if _name == "airflow.exceptions":
            class AirflowFailException(Exception):
                pass

            sys.modules[_name].AirflowFailException = AirflowFailException
