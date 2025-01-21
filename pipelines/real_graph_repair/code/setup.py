from setuptools import setup, find_packages
setup(
    name = 'real_graph_repaired',
    version = '1.0',
    packages = find_packages(include = ('real_graph_repaired*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.29'],
    entry_points = {
'console_scripts' : [
'main = real_graph_repaired.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
