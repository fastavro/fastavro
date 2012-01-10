all: fastavro.so

fastavro.so: fastavro.pyx
	python setup.py build_ext --inplace

clean:
	rm -f fastavro.c fastavro.so

fresh: clean all

.PHONY: all clean fresh
