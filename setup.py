import fastavro

from setuptools import setup
from shutil import copy
from os.path import isfile, getmtime, join
from subprocess import check_call
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils import log
from distutils.core import Extension

cfile = join('fastavro', 'cfastavro.c')
pyfile = join('fastavro', 'pyfastavro.py')

def should_cython():
    try:
        import Cython
    except ImportError:
        return False

    if not isfile(cfile):
        return True

    return getmtime(pyfile) > getmtime(cfile)

if should_cython():
    log.info('Generating {0}'.format(cfile))
    check_call(['cython', pyfile, '-o', cfile])

class try_build_ext(build_ext):
    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except CCompilerError:
            log.warn('Failed to build optional extension, skipping')

cfastavro = Extension('fastavro.cfastavro', [cfile])

setup(
    name='fastavro',
    version=fastavro.__version__,
    description='Fast iteration of AVRO files',
    long_description=open('README.rst').read(),
    author='Miki Tebeka',
    author_email='miki.tebeka@gmail.com',
    license='MIT',
    url='https://bitbucket.org/tebeka/fastavro',
    packages=['fastavro'],
    ext_modules=[cfastavro],
    cmdclass={'build_ext' : try_build_ext},
    zip_safe=False,
)
