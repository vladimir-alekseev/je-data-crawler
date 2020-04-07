from setuptools import setup, find_packages
from data_crawler.version import version

with open("README.md", "r") as file_handler:
    long_description = file_handler.read()

setup(
    name="je-data-crawler",
    description="A data crawler for Jobs Explorer",
    version=version,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vladimir-alekseev/je-data-crawler",
    author="Vladimir Alekseev",
    author_email="vladimir.alekseev@gmail.com",
    license="MIT",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7, <4',
    install_requires=['requests', 'mysql-connector-python'],
    entry_points={
        'console_scripts': [
            'data_crawler=data_crawler.__main__:main',
        ],
    },
)
