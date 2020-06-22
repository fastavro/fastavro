# Makefile for fastavro project
#
# Since we distribute the generated C file for simplicity (and so that end users
# won't need to install cython). You can re-create the C file using this
# Makefile.


ifndef PYTHON
    PYTHON=python
endif

%.c: %.pyx
	cython $(<D)/$(<F)

c_files = fastavro/_read.c fastavro/_write.c fastavro/_schema.c fastavro/_validation.c fastavro/_logical_writers.c

all: $(c_files)

clean:
	rm -fv $(c_files)
	rm -fv fastavro/*.so
	rm -fv fastavro/_*.html

fresh: clean all html

tag:
	./tag.sh

publish:
	./publish.sh

test:
	PATH="${PATH}:${HOME}/.local/bin" tox

html:
	cython -a fastavro/*.pyx

docs:
	pip install -U sphinx sphinx_rtd_theme
	cd docs && make html

.PHONY: all clean fresh publish test docs
