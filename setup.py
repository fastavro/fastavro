from setuptools import setup
from shutil import copy
import fastavro

try:
    from Cython.Distutils import build_ext
    from distutils.extension import Extension

    pyx = 'fastavro/cfastavro.pyx'
    copy('fastavro/pyfastavro.py', pyx)

    ext = Extension('fastavro.cfastavro', [pyx])
    ext_modules = [ext]
    cmdclass = {'build_ext': build_ext}
except ImportError:
    ext_modules = []
    cmdclass = {}


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
    ext_modules=ext_modules,
    cmdclass=cmdclass,
    zip_safe=False,
)
