import setuptools
import sys

sys.path.insert(0, "/home/mpalermo/Repositories/job-dispatcher")


setuptools.setup(
    name="job-dispatcher",
    version="0.1.8",
    description="",
    long_description="",
    packages=["jobdispatcher"],
    package_data={"jobdispatcher": ["packing/*",],},
    install_requires=[],
)
