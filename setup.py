from importlib.machinery import SourceFileLoader
from pathlib import Path

from setuptools import setup

description = '↻ 一个用于 mongodb 数据库转换为各类文件格式的库'
long_description = '↻ 一个用于 mongodb 数据库转换为各类文件格式的库'

version = SourceFileLoader('version', 'mongov/version.py').load_module()
readme = Path(__file__).parent / 'README.md'
if readme.exists():
    long_description = readme.read_text(encoding='utf-8')

setup(
    name='mongov',
    version=version.__version__,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Monitoring',
        'Topic :: System :: Systems Administration',
    ],
    python_requires='>=3.7',
    author='PY-GZKY',
    author_email='341796767@qq.com',
    url='https://github.com/PY-GZKY/mongov',
    license='MIT',
    packages=['mongov'],
    include_package_data=True,
    zip_safe=True,
    entry_points="""
        [console_scripts]
        mongov=mongov.cli:cli
    """,
    install_requires=[
        'click>=6.7',
        'colorama==0.4.4',
        'pandas==1.3.0',
        'pymongo==3.11.4',
        'PyMySQL==0.9.3',
        'python-dotenv==0.19.2',
        'python_dateutil==2.8.2',
        'setuptools==60.0.3',
        'pyarrow==7.0.0',
    ],
)
