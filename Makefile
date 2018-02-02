src_files := presto_helper.py

ve:
	virtualenv --python=python3 ve
	. ve/bin/activate && pip install -r requirements.txt

pep8: ve
	. ve/bin/activate && pep8 $(src_files) --max-line-length=120

pylint: ve
	. ve/bin/activate && pylint -E $(src_files)

check: pep8 pylint
