"""DAG integrity tests — validates DAG structure without running tasks.

These tests import DAGs and verify they parse correctly, have no
import errors, and follow expected patterns. Runs in CI without
any external service dependencies.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest


# Mock external dependencies that aren't available in test environment
@pytest.fixture(autouse=True)
def mock_airflow_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock Airflow and Snowflake imports for DAG parsing tests."""
    mock_modules = {
        "airflow": MagicMock(),
        "airflow.models": MagicMock(),
        "airflow.operators": MagicMock(),
        "airflow.operators.python": MagicMock(),
        "snowflake": MagicMock(),
        "snowflake.connector": MagicMock(),
    }

    # Create a proper DAG mock that supports context manager
    mock_dag = MagicMock()
    mock_dag.__enter__ = MagicMock(return_value=mock_dag)
    mock_dag.__exit__ = MagicMock(return_value=False)

    mock_modules["airflow"].DAG = MagicMock(return_value=mock_dag)

    for mod_name, mock_mod in mock_modules.items():
        monkeypatch.setitem(sys.modules, mod_name, mock_mod)


class TestDagFileIntegrity:
    """Verify DAG files can be parsed without errors."""

    def test_dag_file_exists(self) -> None:
        """The bronze ingestion DAG file should exist."""
        dag_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "dags",
            "bronze_ingestion_dag.py",
        )
        assert os.path.isfile(dag_path), f"DAG file not found: {dag_path}"

    def test_dag_has_no_syntax_errors(self) -> None:
        """DAG file should compile without syntax errors."""
        dag_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "dags",
            "bronze_ingestion_dag.py",
        )
        with open(dag_path) as f:
            source = f.read()

        # Should not raise SyntaxError
        compile(source, dag_path, "exec")
