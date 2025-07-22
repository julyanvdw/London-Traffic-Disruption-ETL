VENV_NAME=pipeline_venv
PYTHON=python3

.PHONY: all setup run test api example clean

all: setup

setup:
	$(PYTHON) -m venv $(VENV_NAME)
	. $(VENV_NAME)/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# Run the pipeline orchestrator
run_pipeline:
	. $(VENV_NAME)/bin/activate && $(PYTHON) pipeline_orchestrator.py

# Run the TUI
run_tui:
	. $(VENV_NAME)/bin/activate && $(PYTHON) pipeline_tui.py

# Run all tests
run_tests:
	. $(VENV_NAME)/bin/activate && $(PYTHON) -m pytest

# Start the FastAPI server
start_api:
	. $(VENV_NAME)/bin/activate && uvicorn service_layer.pipeline_api:app --reload

# Run the example app
run_example:
	. $(VENV_NAME)/bin/activate && $(PYTHON) service_layer/example_app.py

clean:
	rm -rf $(VENV_NAME)
