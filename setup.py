from setuptools import setup, find_packages

Name = "automation"
Description = 'Data test automation'
url = 'https://github.com/novemberautomation2023/Data_validation_tool.git'
version = '1.1'

Required = ['pytest','openpyxl']

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

# Metadata and Project Information:
#
# name: The name of your project.
# version: The version number of your project.
# author, author_email: Information about the project's author.
# description: A short description of your project.
# url: The URL of your project's homepage or repository.
# These details are specified in the setup() function of your setup.py script and are used to provide information about your project.
#
# Package Discovery:
#
# The find_packages() function is often used to automatically discover and include all Python packages in your project. It looks for directories with __init__.py files and includes them as packages.
# Dependencies:
#
# The install_requires parameter in setup() is used to specify the dependencies your project relies on. These dependencies will be installed automatically when someone installs your project using pip.

# Distribution Formats:
#
# setuptools supports various distribution formats, including source distribution (sdist) and binary distribution (bdist). The bdist_wheel format, in particular, is widely used for distributing binary wheels, which are more efficient and faster to install.
# Useful Commands:
#
# python setup.py sdist: Creates a source distribution.
# python setup.py bdist_wheel: Creates a binary distribution (wheel).
# python setup.py install: Installs the package locally.