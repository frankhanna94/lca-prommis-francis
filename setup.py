from setuptools import setup, find_packages
setup(
    name="lca-prommis",
    version="0.1.0",
    package_dir={"": "src"},    
    packages=["src"] + ["src.create_olca_process"],
    install_requires=[
        "fedelemflowlist @ git+https://github.com/FLCAC-Admin/fedelemflowlist",
        "netlolca @ git+https://github.com/NETL-RIC/netlolca",
        "numpy",
        "pubchempy",
        "pymatgen",
        "openpyxl",
    ]
)
