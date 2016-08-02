all: test

.PHONY: clean
clean:
	# Remove the build
	rm -rf build dist
	# And all of our pyc files
	find . -name '*.pyc' -delete
	# And lastly, .coverage files
	find . -name .coverage -delete

.PHONY: qless-core
qless-core:
	# Ensure qless is built
	make -C qless/qless-core/

.PHONY: nose
nose: qless-core
	nosetests --with-coverage

requirements:
	pip freeze | grep -v -e qless-py > requirements.txt

.PHONY: test
test: nose
