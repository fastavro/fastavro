from setuptools import setup
from shutil import copy

try:
    from Cython.Distutils import build_ext
    from distutils.extension import Extension

    pyx = 'fastavro/cfastavro.pyx'
    copy('fastavro/pyfastavro.py', pyx)

    fastavro = Extension('fastavro.cfastavro', [pyx])
    ext_modules = [fastavro]
    cmdclass = {'build_ext': build_ext}
except ImportError:
    ext_modules = []
    cmdclass = {}

setup(
    name='fastavro',
    version='0.2.0',
    description='Fast iteration of AVRO files',
    author='Miki Tebeka',
    author_email='miki.tebeka@gmail.com',
    license='MIT',
    url='https://bitbucket.org/tebeka/fastavro',
    packages=['fastavro'],
    ext_modules=ext_modules,
    cmdclass=cmdclass,
    zip_safe=False,
)
