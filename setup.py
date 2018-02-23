from setuptools import setup, find_packages

setup(

    name='ExperimentalDataReader',
    version='0.5',
    packages=find_packages("DataReader"),
    url='https://github.intel.com/liqunjia/ExperimentalDataReader',
    license='BSD',
    author='Litrin Jiang',
    author_email='litrin.jiang@intel.com',
    description='python lib for raw test data file reading',

    install_requires=[
        "pandas",
        "xlwt",
    ],
)
