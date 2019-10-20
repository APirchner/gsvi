from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='gsvi',
    version='0.1.0',
    url='https://github.com/APirchner/gsvi',
    license=license,
    author='Andreas Pirchner',
    author_email='andreas.pirchner1990@gmail.com',
    description='Interface for Google Trends time-series',
    long_description=readme,
    packages=find_packages(exclude=('tests', 'docs'))
)
