import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="pyslicer",
    version="2.0.2",
    author="SlicingDice LLC",
    author_email="help@slicingdice.com",
    description="Official Python 3 client for SlicingDice, Data Warehouse and "
                "Analytics Database as a Service.",
    install_requires=["aiohttp", "six", "ujson"],
    license="BSD",
    keywords="slicingdice slicing dice data analysis analytics database",
    packages=[
        'pyslicer',
        'pyslicer.core',
        'pyslicer.utils',
    ],
    package_dir={'pyslicer': 'pyslicer'},
    long_description=read('README.md'),
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
)
