try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from distutils.core import Extension
import distutils.log as log
from os.path import join
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, \
    DistutilsPlatformError
from sys import version_info

import fastavro

def extension(base):
    cmodule = '_{0}'.format(base)
    cfile = join('fastavro', '{0}.c'.format(cmodule))

    return Extension('fastavro.{0}'.format(cmodule), [cfile])


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


if version_info[:2] > (2, 6):
    install_requires = []
else:
    install_requires = ['argparse']


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
    ext_modules=[extension('reader'), extension('six')],
    cmdclass={'build_ext': maybe_build_ext},
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'fastavro = fastavro.__main__:main',
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=install_requires
)
