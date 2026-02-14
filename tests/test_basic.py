"""Basic smoke tests to ensure imports work correctly."""

import pytest


def test_imports():
    """Test that core modules can be imported."""
    try:
        from src.utils import config, logging_setup
        from src.api import auth, client
        from src.checkpoint import manager
        from src.spark_jobs import extract_job

        assert True
    except ImportError as e:
        pytest.fail(f"Import failed: {e}")


def test_python_version():
    """Test Python version is compatible."""
    import sys

    assert sys.version_info >= (3, 9)
    assert sys.version_info < (3, 13)
