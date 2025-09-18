PY?=python3
PKG=src

install:
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e .[dev] prometheus-client pyspark

lint:
	flake8 $(PKG)

test:
	pytest -q

run:
	APP_ENV=dev $(PY) -m pyspark_interview_project.pipeline_stages.run_pipeline --ingest-metrics-json --with-dr

fmt:
	black $(PKG) && isort $(PKG)

hooks:
	pre-commit install
