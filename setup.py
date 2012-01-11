from setuptools import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

fastavro = Extension("fastavro", ["fastavro.pyx"])

setup(
    name='fastavro',
    version='0.1.0',
    description='Fast iteration of AVRO files',
    author='Miki Tebeka',
    author_email='miki.tebeka@gmail.com',
    license='MIT',
    url='https://bitbucket.org/tebeka/fastavro',
    ext_modules=[fastavro],
    cmdclass={'build_ext': build_ext},
    install_requires=['cython'],
)
