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

c_files = fastavro/_six.c fastavro/_read.c fastavro/_write.c fastavro/_schema.c

all: $(c_files)

clean:
	rm -fv $(c_files)
	rm -fv fastavro/*.so

fresh: clean all

publish:
	./publish.sh

test:
	PATH="${PATH}:${HOME}/.local/bin" tox

.PHONY: all clean fresh publish test
