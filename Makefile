all:
	echo Nothing to do

.PHONY: clean
clean:
	# Remove the build
	sudo rm -rf build dist
	# And all of our pyc files
	find . -name '*.pyc' | xargs -n 100 rm
	# And lastly, .coverage files
	find . -name .coverage | xargs rm

.PHONY: qless-core
qless-core:
	# Ensure qless is built
	make -C qless/qless-core/

.PHONY: nose
nose: qless-core
	rm -rf .coverage
	nosetests --exe --cover-package=qless --with-coverage --cover-branches -v

.PHONY: nose3
nose3: qless
	rm -rf .coverage
	nosetests3 --exe --cover-package=qless --with-coverage --cover-branches -v

.PHONY: test
test: nose nose3
