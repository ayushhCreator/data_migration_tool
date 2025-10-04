# Place this file at: ./setup.py (root directory)
from setuptools import setup, find_packages

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

# Dynamic version reading from __init__.py
exec(open('data_migration_tool/__init__.py').read())

setup(
    name='data_migration_tool',
    version=__version__,
    description='Migrate Data from different sources to Frappe/ERPNext',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Ayush Raj',
    author_email='araj09510@gmail.com',
    url='https://github.com/ayushhCreator/data_migration_tool',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
    python_requires='>=3.10',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Framework :: Frappe',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
    ],
)
