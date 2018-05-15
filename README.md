# fastavro
[![Build Status](https://travis-ci.org/tebeka/fastavro.svg?branch=master)](https://travis-ci.org/tebeka/fastavro)
[![Documentation Status](https://readthedocs.org/projects/fastavro/badge/?version=latest)](http://fastavro.readthedocs.io/en/latest/?badge=latest)



Because the Apache Python `avro` package is written in pure Python, it is
relatively slow. In one test case, it takes about 14 seconds to iterate through
a file of 10,000 records. By comparison, the JAVA `avro` SDK reads the same file in
1.9 seconds.

The `fastavro` library was written to offer performance comparable to the Java
library. With regular CPython, `fastavro` uses C extensions which allow it to
iterate the same 10,000 record file in 1.7 seconds. With PyPy, this drops to 1.5
seconds (to be fair, the JAVA benchmark is doing some extra JSON
encoding/decoding).

`fastavro` supports the following Python versions:

* Python 2.7
* Python 3.4
* Python 3.5
* Python 3.6
* PyPy
* PyPy3

[Cython]: http://cython.org/

# Documentation

Documentation is available at http://fastavro.readthedocs.io/en/latest/

# Installing
`fastavro` is available both on [PyPi](http://pypi.python.org/pypi)

    pip install fastavro

and on [conda-forge](https://conda-forge.github.io) `conda` channel.

    conda install -c conda-forge fastavro

# Contributing

* Bugs and new feature requests typically start as github issues where they can be discussed. I try to resolve these as time affords, but PRs are welcome from all.
* Get approval from discussing on the github issue before opening the pull request
* Tests must be passing for pull request to be considered

Developer requirements can be installed with `pip install -r developer_requirements.txt`.
If those are installed, you can run the tests with `./run-tests.sh`. If you have trouble
installing those dependencies, you can run `docker build .` to run the tests inside
a docker container. This won't test on all versions of python or on pypy, so it's possible
to still get CI failures after making a pull request, but we can work through those errors
if/when they happen.

### Releasing

We release both to [pypi][pypi] and to [conda-forge][conda-forge].

We assume you have [twine][twine] installed and that you've created your own
fork of [fastavro-feedstock][feedstock].

* Make sure the tests pass
* Run `make tag`
* Copy the windows build artifacts for the new version from
  https://ci.appveyor.com/project/scottbelden/fastavro to the `dist` folder
* Copy the linux build artifacts for the new version from
  https://github.com/tebeka/fastavro/releases/tag/ to the `dist` folder
* Run `make publish`
* Note the sha signature emitted at the above
* Switch to feedstock directory and edit `recipe/meta.yaml`
    - Update `version` and `sha256` variables at the top of the file
    - Run `python recipe/test_recipe.py`
    - Submit a [PR][pr]

[conda-forge]: https://conda-forge.org/
[feedstock]: https://github.com/conda-forge/fastavro-feedstock
[pr]: https://conda-forge.org/#update_recipe
[pypi]: https://pypi.python.org/pypi
[twine]: https://pypi.python.org/pypi/twine


# Changes

See the [ChangeLog]

[ChangeLog]: https://github.com/tebeka/fastavro/blob/master/ChangeLog

# Contact

[Project Home](https://github.com/tebeka/fastavro)
