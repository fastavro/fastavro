import fastavro

from setuptools import setup
from distutils.core import Extension
import distutils.log as log
from os.path import join
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, \
    DistutilsPlatformError

cfile = join('fastavro', 'cfastavro.c')

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
              IOError)

class maybe_build_ext(build_ext):
    '''This class allows C extension building to fail.'''

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            log.info('cannot bulid C extension, will continue without.')

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            log.info('cannot bulid C extension, will continue without.')

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
    ext_modules=[Extension('fastavro.cfastavro', [cfile])],
    cmdclass={'build_ext' : maybe_build_ext},
    zip_safe=False,
)
