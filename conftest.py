"""
conftest.py — pytest configuration

Adds the dags/ directory to sys.path so that
'from scripts.extract import extract' works when running:
  pytest dags/tests/test_etl.py -v
from the project root.
"""
import sys
import os

# Add dags/ to path so scripts.* imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
