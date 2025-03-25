import json
import pytest
from jsonschema import validate, ValidationError
import os

env_placeholder = "<param_env>"
paths_to_check = [
    f"../configs/{env_placeholder}/extract/objects_to_extract"
]

def load_json(file_path):
    """Load a JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_json(data, schema):
    """Validate JSON data against a given schema."""
    validate(instance=data, schema=schema)

@pytest.mark.parametrize("path", paths_to_check)
def test_dev_config(path):
    """Test JSON validation for all specified paths."""
    env = "dev"  # Change this to dynamically test different environments

    json_file = path.replace(env_placeholder, env) + "/AdventureWorks.json"
    schema_file = path.replace(env_placeholder, env) + ".json"

    if not os.path.exists(json_file) or not os.path.exists(schema_file):
        pytest.skip(f"Skipping test: {json_file} or {schema_file} not found.")

    json_data = load_json(json_file)
    schema_data = load_json(schema_file)

    validate_json(json_data, schema_data)


@pytest.mark.parametrize("path", paths_to_check)
def test_prod_config(path):
    """Test JSON validation for all specified paths."""
    env = "prod"  # Change this to dynamically test different environments

    json_file = path.replace(env_placeholder, env) + "/AdventureWorks.json"
    schema_file = path.replace(env_placeholder, env) + ".json"

    if not os.path.exists(json_file) or not os.path.exists(schema_file):
        pytest.skip(f"Skipping test: {json_file} or {schema_file} not found.")

    json_data = load_json(json_file)
    schema_data = load_json(schema_file)

    validate_json(json_data, schema_data)