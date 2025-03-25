# Databricks notebook source
# MAGIC %pip install jsonschema

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import sys
import os


print(os.getcwd())
# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
