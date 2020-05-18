import setuptools

setuptools.setup(
    name='lod-api',
    version='0.5.0',
    description='API to provide LOD from Elasticsearch via REST interface',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='SLUB LOD Team',
    author_email='lod.team@slub-dresden.de',
    license=open('LICENSE').read(),
    url="https://data.slub-dresden.de",
    packages=setuptools.find_packages(exclude=('tests')),
    install_requires=open('requirements.txt').read().split('\n'),
    entry_points={
        'console_scripts': [
            'lod-api=lod_api.cli:main',
        ]
    },
)
