#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=7.0', 'loguru', 'pyyaml', 'pyaml',
    'pydantic<2.0', 'python-dotenv', 'pydantic-yaml<1.0',
    'pyDataverse @ git+https://github.com/Jerry-Ma/pyDataverse.git@dvpipe',
    'pandas',
    'dagster', 'dagit',
    'astropy',
    ]

test_requirements = ['pytest>=3', ]

setup(
    author="Zhiyuan Ma",
    author_email='zhiyuanma@umass.edu',
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
    description="Dataverse integration for data pipelines.",
    entry_points={
        'console_scripts': [
            'dvpipe=dvpipe.cli:main',
        ],
    },
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='dvpipe',
    name='dvpipe',
    packages=find_packages(include=['dvpipe', 'dvpipe.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/toltec-astro/dvpipe',
    version='0.1.0',
    zip_safe=False,
)
