from setuptools import setup, find_packages
from setuptools.command import easy_install

def install_script(self, dist, script_name, script_text, dev_path=None):
    script_text = easy_install.get_script_header(script_text) + (
        ''.join(script_text.splitlines(True)[1:]))

    self.write_script(script_name, script_text, 'b')

easy_install.easy_install.install_script = install_script

setup(
    name     = 'jeque',
    version  = '0.2dev',
    author   = 'Anton Bobrov',
    author_email = 'bobrov@vl.ru',
    description = 'Simple job queue with priority and blocked acknowledgment result waiting',
    zip_safe   = False,
    packages = find_packages(exclude=['tests']),
    scripts = ['bin/jeque'],
    url = 'http://github.com/baverman/jeque',
)