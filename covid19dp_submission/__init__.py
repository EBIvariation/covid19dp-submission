import os
import covid19dp_submission


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # This is your Project Root

NEXTFLOW_DIR = os.path.join(os.path.dirname(os.path.abspath(covid19dp_submission.__file__)), 'nextflow')

__version__ = open(os.path.join(os.path.dirname(os.path.abspath(covid19dp_submission.__file__)),
                                'VERSION')).read().strip()
