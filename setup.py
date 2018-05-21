import ast
import os
import re
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from setuptools import Extension

# publish.sh should set this variable to 1.
try:
    USE_CYTHON = int(os.getenv('FASTAVRO_USE_CYTHON'))
except TypeError:
    USE_CYTHON = False

ext = '.pyx' if USE_CYTHON else '.c'

# See http://setuptools.readthedocs.io/en/latest/setuptools.html\
#     #distributing-extensions-compiled-with-pyrex
ext_modules = []
if not hasattr(sys, 'pypy_version_info'):
    ext_modules += [
        Extension('fastavro._read', ["fastavro/_read" + ext]),
        Extension('fastavro._schema', ["fastavro/_schema" + ext]),
        Extension('fastavro._six', ["fastavro/_six" + ext]),
        Extension('fastavro._write', ["fastavro/_write" + ext]),
        Extension('fastavro._validation', ["fastavro/_validation" + ext]),
    ]


def version():
    pyfile = 'fastavro/__init__.py'
    with open(pyfile) as fp:
        data = fp.read()

    match = re.search('__version_info__ = (\(.*\))', data)
    assert match, 'cannot find version in {}'.format(pyfile)
    vinfo = ast.literal_eval(match.group(1))
    return '.'.join(str(v) for v in vinfo)


setup_requires = []
if sys.version_info[:2] > (2, 6):
    install_requires = []
else:
    install_requires = ['argparse']
if not hasattr(sys, 'pypy_version_info'):
    cpython_requires = [
        # Setuptools 18.0 properly handles Cython extensions.
        'setuptools>=18.0',
    ]
    if USE_CYTHON:
        cpython_requires += [
            'Cython',
        ]
    install_requires += cpython_requires
    setup_requires += cpython_requires

setup(
    name='fastavro',
    version=version(),
    description='Fast read/write of AVRO files',
    long_description=open('README.md').read(),
    author='Miki Tebeka',
    author_email='miki.tebeka@gmail.com',
    license='MIT',
    url='https://github.com/tebeka/fastavro',
    packages=['fastavro'],
    ext_modules=ext_modules,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'fastavro = fastavro.__main__:main',
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],
    install_requires=install_requires,
    extras_require={
        'snappy': ['python-snappy'],
    },
    tests_require=['pytest', 'flake8', 'check-manifest'],
    setup_requires=setup_requires
)
