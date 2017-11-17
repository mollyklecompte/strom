from setuptools import setup, find_packages

setup(
    name='strom-cli',
    version='0.0.1',
    author='Adrian Agnic',
    author_email='adrian@tura.io',
    description='CLI tool for use with Strom',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['click', 'requests'],
    entry_points='''
    [console_scripts]
    dstream=interface.tool:dstream
    ''',
)
