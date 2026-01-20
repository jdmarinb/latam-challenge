# Makefile for PowerShell environments (Windows)

# Define the virtual environment directory
VENV_DIR := .venv
# Define the python interpreter from the virtual environment
ifeq ($(OS),Windows_NT)
    PYTHON := $(VENV_DIR)\Scripts\python.exe
else
    PYTHON := $(VENV_DIR)/bin/python
endif

# Phony targets don't represent files
.PHONY: all setup install test lint clean commit

# Default target: runs when you just type 'make'
all: setup

# Target to install uv if not present
install-uv:
	@uv --version > /dev/null 2>&1 || (echo "--- Installing uv ---" && curl -LsSf https://astral.sh/uv/install.sh | sh)

# Target to set up the virtual environment and install dependencies
setup: install-uv $(PYTHON)

$(PYTHON): requirements-dev.txt
	@echo "--- Creating virtual environment and installing dependencies with uv ---"
	uv venv $(VENV_DIR) --python 3.12 --seed --allow-existing
	uv pip install -r requirements-dev.txt
	@echo "--- Installing pre-commit hooks ---"
	uv run pre-commit install
	@echo "--- Installing commitizen hooks ---"
	uv run pre-commit install --hook-type commit-msg
	@echo ""
	@echo ">>> Environment setup complete. Activate it by running: source $(VENV_DIR)/bin/activate <<<"

# Target to run tests
test:
	@echo "--- Running tests ---"
	uv run pytest

# Target to run linter
lint:
	@echo "--- Running linter ---"
	uv run ruff check . --fix --unsafe-fixes --exit-non-zero-on-fix

# Target to commit using commitizen and gitmoji
commit:
	@echo "--- Committing with commitizen (cz_gitmoji) ---"
	uv run cz commit

# Clean up generated files and virtual environment
clean:
	@echo "--- Cleaning up project ---"
	@if [ -d "$(VENV_DIR)" ]; then rm -rf $(VENV_DIR); fi
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
