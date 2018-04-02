from pybuilder.core import use_plugin, init,task


use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.distutils")


name = "WikiTopK"
default_task = ["install_dependencies","publish"]


@init
def set_properties(project):
    project.build_depends_on('mockito')
    project.depends_on('luigi')
    project.depends_on('pandas')

