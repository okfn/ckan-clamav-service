import os

from setuptools import find_packages, setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


def _get_description():
    try:
        path = os.path.join(os.path.dirname(__file__), "README.md")
        with open(path, encoding="utf-8") as f:
            return f.read()
    except IOError:
        return ""


setup(
    name="ckan-clamav-service",
    version="0.6.0",
    author="OKFN",
    license="AGPL-3.0",
    url="https://github.com/okfn/ckan-clamav-service",
    packages=find_packages(),
    include_package_data=True,
    description="A CKAN service for scanning uploads with Clam AV",
    long_description=_get_description(),
    long_description_content_type="text/markdown",
    install_requires=[],
)
