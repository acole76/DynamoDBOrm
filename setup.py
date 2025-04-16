from setuptools import setup, find_packages

setup(
    name="DynamoDbOrm",
    version="0.1.0",
    author="Anthony Cole",
    author_email="acole76[at]gmail.com",
    description="Orm for AWS DynamoDB.",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.0.0",
        "marshmallow>=3.23.3"
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)