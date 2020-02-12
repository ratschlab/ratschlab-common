#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup_requirements = ['pytest-runner']

with open('requirements.txt') as f:
    requirements = list(f.readlines())

extras_require = {
  'spark': ['pyspark >= 2.3'],
  'dask': ['dask[complete] >= 1.2.0', 'distributed >= 1.22']
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

test_requirements = ['pytest']

setup(
    author="ETH Zurich, Biomedical Informatics Group",
    author_email='marc.zimmermann@inf.ethz.ch',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    description="Small library of common functionalities used in various projects in the ratschlab",
    entry_points={
        'console_scripts': [
            'bigmatrix_repack=ratschlab_common.scripts.bigmatrix_repack:main',
            'export_db_to_files=ratschlab_common.scripts.export_db_to_files:main',
            'pq_tool=ratschlab_common.parquet_tools.pq_tool:pq_tool'
        ],
    },
    install_requires=requirements,
    extras_require=extras_require,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords='ratschlab_common',
    name='ratschlab_common',
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ratschlab/ratschlab-common',
    version='0.2.0',
    zip_safe=False,
)
