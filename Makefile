PY=python

ingest:
	$(PY) -m src.etl.ingest

silver:
	$(PY) -m src.etl.silver

gold:
	$(PY) -m src.etl.gold_features

train:
	$(PY) -m src.ml.train

stream:
	$(PY) -m src.ml.score_stream

all: ingest silver gold train
	@echo "✅ Batch pipeline done; run 'make stream' for realtime scoring."
