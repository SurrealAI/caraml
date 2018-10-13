from setuptools import setup, find_packages


setup(
    name='caraml',
    version='0.0.1',
    author='Surreal AI team',
    url='https://github.com/SurrealAI/caraml',
    description='Carefree Accelerated Messaging Library: protocol layer '
                'for distributed machine learning',
    keywords=['Distributed', 'Messaging', 'Machine Learning'],
    license='GPLv3',
    packages=[
        package for package in find_packages() if package.startswith("caraml")
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Environment :: Console",
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        "pyarrow",
        "nanolog",
        "zmq",
    ],
    python_requires='>=3.5',
    include_package_data=True,
    zip_safe=False
)
