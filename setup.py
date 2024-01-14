from setuptools import setup, find_packages

Name = "automation"
Description = 'Data test automation'
url = 'https://github.com/novemberautomation2023/Data_validation_tool.git'
version = '1.1'

Required = ['pytest']

setup(
    name=Name,
    description=Description,
    url=url,
    install_requires= Required,
    version=version,
    packages=find_packages(),
    package_dir = {'Data_validation_tool':'Data_validation_tool'},
    include_package_data=True,
    package_data={
        'Config':['*.json'],
        'Config':['*.csv']

    }
)

#pip install setuptool
#python setup.py sdist bdist_wheel

