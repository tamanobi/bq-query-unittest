import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bq-query-logic-test", # Replace with your own username
    version="0.0.1",
    author="tamanobi",
    author_email="tamanobi@gmail.com",
    description="Test BigQuery query using BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tamanobi/https://github.com/tamanobi/bq-query-unittest",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
