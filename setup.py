import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pydags",
    version="0.1.0",
    author="David Torpey",
    author_email="torpey.david93@gmail.com",
    description="A simple, lightweight, extensible DAG framework for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DavidTorpey/pydags",
    project_urls={
        "Bug Tracker": "https://github.com/DavidTorpey/pydags/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    license='GPLv3'
)