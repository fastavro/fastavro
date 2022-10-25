import ast
import os
import re
import sys
from setuptools import setup, Extension

try:
    import Cython
except ImportError:
    ext = ".c"
else:
    ext = ".pyx"

ext_modules = []
if not hasattr(sys, "pypy_version_info"):
    ext_modules += [
        Extension("fastavro._read", ["fastavro/_read" + ext]),
        Extension("fastavro._schema", ["fastavro/_schema" + ext]),
        Extension("fastavro._write", ["fastavro/_write" + ext]),
        Extension("fastavro._validation", ["fastavro/_validation" + ext]),
        Extension("fastavro._logical_readers", ["fastavro/_logical_readers" + ext]),
        Extension("fastavro._logical_writers", ["fastavro/_logical_writers" + ext]),
    ]


def version():
    pyfile = "fastavro/__init__.py"
    with open(pyfile) as fp:
        data = fp.read()

    match = re.search("__version_info__ = (\(.*\))", data)
    assert match, f"cannot find version in {pyfile}"
    vinfo = ast.literal_eval(match.group(1))
    return ".".join(str(v) for v in vinfo)


setup(
    name="fastavro",
    version=version(),
    description="Fast read/write of AVRO files",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Miki Tebeka",
    author_email="miki.tebeka@gmail.com",
    license="MIT",
    url="https://github.com/fastavro/fastavro",
    packages=["fastavro", "fastavro.io", "fastavro.repository"],
    ext_modules=ext_modules,
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "fastavro = fastavro.__main__:main",
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.7",
    extras_require={
        "codecs": ["python-snappy", "zstandard", "lz4"],
        "snappy": ["python-snappy"],
        "zstandard": ["zstandard"],
        "lz4": ["lz4"],
    },
    package_data={"fastavro": ["py.typed"]},
)
