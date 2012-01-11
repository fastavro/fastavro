from setuptools import setup

try:
    from distutils.extension import Extension
    from Cython.Distutils import build_ext
    fastavro = Extension('fastavro.cfastavro', ['fastavro/cfastavro.pyx'])
    ext_modules = [fastavro]
    cmdclass = {'build_ext': build_ext}
except ImportError:
    ext_modules = []
    cmdclass = {}

setup(
    name='fastavro',
    version='0.1.0',
    description='Fast iteration of AVRO files',
    author='Miki Tebeka',
    author_email='miki.tebeka@gmail.com',
    license='MIT',
    url='https://bitbucket.org/tebeka/fastavro',
    ext_modules=ext_modules,
    cmdclass=cmdclass,
)
