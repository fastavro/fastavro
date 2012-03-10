# Makefile for fastavro project
#
# Since we distribute the generated C file for simplicity (and so that end users
# won't need to install cython). You can re-create the C file using this
# Makefile.

c_file2 = fastavro/cfastavro2.c
c_file3 = fastavro/cfastavro3.c
py_file = fastavro/pyfastavro.py
pyc_file2 = fastavro/cfastavro2.py
pyc_file3 = fastavro/cfastavro3.py

all: $(c_file2) $(c_file3)

# FIXME: Unite to one goal
$(c_file2): $(py_file)
	cp $(py_file) $(pyc_file2)
	cython -2 $(pyc_file2)
	rm $(pyc_file2)

$(c_file3): $(py_file)
	cp $(py_file) $(pyc_file3)
	cython -3 $(pyc_file3)
	rm $(pyc_file3)
