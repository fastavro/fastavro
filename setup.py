import fastavro

from setuptools import setup
from distutils.core import Extension
from os.path import join

cfile = join('fastavro', 'cfastavro.c')

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
    ext_modules=[Extension('fastavro.cfastavro', [cfile], optional=True)],
    zip_safe=False,
)
