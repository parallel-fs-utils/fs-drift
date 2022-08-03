from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="fs-drift",
    version="v0.2",
    description="Scripts and libs for Beaker XML FS generation.",
    long_description=long_description,
    url="https://github.com/parallel-fs-utils/fs-drift",
    keywords=[],
    include_package_data=True,
    packages=find_packages(),
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    install_requires=[
        "pyaml",
        "numpy"
    ],
    entry_points={
        'console_scripts': [
            'fs-drift = fs_drift.__main__:run_workload',
        ]
    }
)
