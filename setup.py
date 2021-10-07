from setuptools import setup, find_packages
import os
import re

classifiers = [
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.7',
    'License :: OSI Approved :: MIT License',
    'Topic :: Scientific/Engineering',
    'Topic :: Scientific/Engineering :: Artificial Intelligence',
]

# Get the long description from the README file
with open('README.md', 'r', encoding='utf8') as fh:
    long_description = fh.read()

# Get version string from module
init_path = os.path.join(os.path.dirname(__file__), 'EasyTwitterAPI/__init__.py')
with open(init_path, 'r', encoding='utf8') as f:
    version = re.search(r"__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M).group(1)

setup(
    name='EasyTwitterAPI',
    version=version,
    description='Easy to use wrapper for the Twitter API in PyTorch',
    author='Pablo Sanchez-Martin',
    author_email='psanch2103@gmail.com',
    license='MIT License',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/psanch21/EasyTwitterAPI',
    classifiers=classifiers,
    keywords=['Multitask Learning', 'Gradient Alignment', 'Gradient Interference', 'Negative Transfer', 'Pytorch',
              'Positive Transfer', 'Gradient Conflict'],
    packages=find_packages(exclude=['local', 'test', 'environments']),
    python_requires='>=3.7',
    install_requires=[],
)
