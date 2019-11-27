import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="dikt",
    version="0.0.1",
    author="Maixent Chenebaux",
    author_email="max.chbx@gmail.com",
    description="Dictionary read on disk",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kerighan/dikt",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=["tqdm", "numpy"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.5"
)
