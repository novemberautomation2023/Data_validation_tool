from setuptools import setup, find_packages

Name = "automation"
Description = 'Data test automation'
url = 'https://github.com/novemberautomation2023/Data_validation_tool.git'
version = '1.0'

Required = ['pytest']

setup(
    name=Name,
    description=Description,
    url=url,
    install_requires= Required,
    version=version,
    packages=find_packages(),
    package_dir = {'Data_validation_tool':'Data_validation_tool'}
)

