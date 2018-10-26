from setuptools import setup

setup(
    name='sml-upload-download',
    version='0.1',
    py_modules=['sml_upload_download'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        sml-upload-download=sml_upload_download:sml_upload_download
    ''',
)
