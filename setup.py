from setuptools import setup, find_packages


install_required = list()

with open('requirements2.txt') as f:
    required = f.read().splitlines()

setup(
    name='EasyTwitterAPI',
    version='1.0.0',
    #packages=['probvis', 'probvis.distributions', 'probvis.graphical_models'],
    packages=find_packages(exclude=['local', 'test', 'environments']),
    dependency_links=['https://github.com/twintproject/twint.git@69e91c861bf3b809aa43afb448c5111d03794d98#egg=twint'],
    install_requires=required,
    url='https://github.com/psanch21/EasyTwitterAPI',
    license='MIT License',
    author='Pablo Sanchez',
    author_email='psanch2103@gmail.com',
    description='Get data from Twitter',
    python_requires='>=3.7',
)