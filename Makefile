clean:
	# Remove the build
	sudo rm -rf build dist
	# And all of our pyc files
	find . -name '*.pyc' | xargs -n 100 rm
	# And lastly, .coverage files
	find . -name .coverage | xargs rm

nose:
	# Ensure qless is built
	make -C qless/qless-core/
	rm -rf .coverage
	nosetests --exe --cover-package=qless --with-coverage --cover-branches -v

test: nose
