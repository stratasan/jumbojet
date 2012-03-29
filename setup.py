from setuptools import setup

NAME = "jumbojet"
DESCRIPTION = "Parse CSV files and generate json/django orm models"
AUTHOR = "Matt George"
AUTHOR_EMAIL = "mgeorge@stratasan.com"
URL = ""
VERSION = '0.1.0'


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=open("README.md").read(),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license="BSD",
    url=URL,
    packages=['jumbojet',],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.7',
    ],
    entry_points={
        'console_scripts': [
            'jet = jumbojet.cli:main',
        ],
    }
)
