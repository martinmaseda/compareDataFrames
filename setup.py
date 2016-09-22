from setuptools import setup

setup(name='workingWithDataFrames',
      version='0.1',
      description='Working smart with pandas DataFrames',
      url='https://github.com/martinmaseda/workingWithDataFrames',
      author='Ivan Martin Maseda',
      author_email='martin.maseda@gmail.com',
      packages=['workingWithDataFrames'],
      license="MIT",
      zip_safe=False,
      install_requires=[
          'pandas'
      ])