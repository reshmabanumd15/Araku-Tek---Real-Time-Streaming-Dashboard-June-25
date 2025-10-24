install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

lint:
	flake8 src

test:
	pytest -q

zip:
	zip -r dist.zip . -x '*.venv*' -x '*.git*' -x 'dist.zip'
