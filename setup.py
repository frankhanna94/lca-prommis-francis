from setuptools import setup, find_packages
setup(
    name="lca-prommis",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "fedelemflowlist @ git+https://github.com/FLCAC-Admin/fedelemflowlist",
        "netlolca @ git+https://github.com/NETL-RIC/netlolca",
        "numpy",
        "pubchempy",
        "pymatgen",
        "openpyxl",
    ]
)
