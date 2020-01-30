from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='gsvi',
    version='v0.2',
    url='https://github.com/APirchner/gsvi',
    download_url = 'https://github.com/APirchner/gsvi/archive/v2.0.tar.gz',
    license='MIT',
    author='Andreas Pirchner',
    author_email='andreas.pirchner1990@gmail.com',
    description='Interface for the Google Trends time-series widget',
    keywords=['Google Trends', 'search volume', 'google search volume'],
    long_description=readme,
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3.7'
)
