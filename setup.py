import glob
from distutils.core import setup
from os.path import join, abspath, dirname

base_dir = dirname(__file__)
requirements_txt = join(base_dir, 'requirements.txt')
requirements = [l.strip() for l in open(requirements_txt) if l and not l.startswith('#')]

version = open(join(base_dir, 'covid19dp_submission', 'VERSION')).read().strip()

setup(
    name='covid19dp_submission',
    packages=['covid19dp_submission', 'covid19dp_submission.steps', 'covid19dp_submission.steps.vcf_vertical_concat'],
    package_data={'covid19dp_submission': ['VERSION', 'etc/*', 'nextflow/*']},
    version=version,
    license='Apache',
    description='EBI EVA - Covid19 Data portal submission processing tools',
    url='https://github.com/EBIVariation/covid19dp-submission',
    keywords=['ebi', 'eva', 'python', 'covid19dp', 'submission'],
    install_requires=requirements,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ],
    scripts=glob.glob(join(base_dir, 'bin', '*.py'))
)
