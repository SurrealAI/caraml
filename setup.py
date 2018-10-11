import os
from setuptools import setup


# def read(fname):
#     with open(os.path.join(os.path.dirname(__file__), fname)) as f:
#         return f.read().strip()


setup(
    name='caraml',
    version='0.0.1',
    author='Surreal AI team',
    url='https://github.com/SurrealAI/caraml',
    description='Carefree Accelerated Messaging Library: protocol layer '
                'for distributed machine learning',
    # long_description=read('README.rst'),
    keywords=['Distributed', 'Messaging', 'Machine Learning'],
    license='GPLv3',
    packages=['caraml'],
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
