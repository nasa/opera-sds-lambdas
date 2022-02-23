"""
Copyright (c) 2019 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import glob
import os
import shlex
import subprocess
import sys
import shutil

import setuptools
import setuptools.command.sdist
import six

WHEELHOUSE = "wheelhouse"
DIST = "dist"


class Package(setuptools.Command):
    """Package Code and Dependencies into wheelhouse"""
    description = "Run wheels for dependencies and submodules dependencies"

    ARCHIVE_NAME = "lambda-event-misfire-handler"

    user_options = [
        ('version=', 'v', 'version release'),
        ('lambda-func=', 'l', 'Path to the Lambda function to softlink to '
                         'lambda_function.py'),
        ('workspace=', 'w', 'Workspace directory. '
                            'Default is current working dir.'),
        ('package-dir=', 'p', 'Directory where the lambda package will be '
                              'located.')
    ]

    def __init__(self, dist):
        self.dist = dist
        setuptools.Command.__init__(self, dist)

    def initialize_options(self):
        self.version = None
        self.lambda_func = None
        self.workspace = None
        self.package_dir = None

    def finalize_options(self):
        """Post-process options."""
        if self.version is None:
            raise Exception("Parameter --version is missing")
        if self.lambda_func is None:
            raise Exception("Parameter --lambda-func is missing")
        if self.workspace is None:
            self.workspace = os.getcwd()
        self.workspace = os.path.abspath(self.workspace)
        if self.package_dir is None:
            raise Exception("Parameter --package-dir is missing")
        else:
            self.package_dir = os.path.abspath(self.package_dir)

    def localize_requirements(self):
        """
        After the package is unpacked at the target destination, the
        requirements can be installed locally from the wheelhouse folder using
        the option --no-index on pip install which ignores package index
        (only looking at --find-links URLs instead).
        --find-links <url | path> looks for archive from url or path.
        Since the original requirements.txt might have links to a non pip repo
        such as github (https) it will parse the links for the archive from a
        url and not from the wheelhouse. This functions creates a new
        requirements.txt with the only name and version for each of the
        packages, thus eliminating the need to fetch / parse links from http
        sources and install all archives from the wheelhouse.
        """
        dependencies = open("requirements.txt").read().split("\n")
        local_dependencies = []

        for dependency in dependencies:
            if dependency:
                if "egg=" in dependency:
                    pkg_name = dependency.split("egg=")[-1]
                    local_dependencies.append(pkg_name)
                elif "git+" in dependency:
                    pkg_name = dependency.split("/")[-1].split(".")[0]
                    local_dependencies.append(pkg_name)
                else:
                    local_dependencies.append(dependency)

        print("local packages in wheel: %s", local_dependencies)
        self.execute("mv requirements.txt requirements.orig")

        with open("requirements.txt", "w") as requirements_file:
            # filter is used to remove empty list members (None).
            requirements_file.write("\n".join(
                filter(None, local_dependencies)))

    def execute(self, command, cwd=None):
        """
        The execute command will loop and keep on reading the stdout and check
        for the return code and displays the output in real time.
        """

        print("Running shell command: ", command)

        try:
            return_code = subprocess.check_call(shlex.split(command),
                                                stdout=sys.stdout,
                                                stderr=sys.stderr, cwd=cwd)
        except subprocess.CalledProcessError as e:
            return_code = e.returncode
            six.raise_from(IOError(
                "Shell commmand `%s` failed with return code %d." % (
                    command, return_code)), e)

        return return_code

    def run_os_commands(self, commands, cwd=None):
        for command in commands:
            self.execute(command, cwd=cwd)

    def restore_requirements_txt(self):
        if os.path.exists("requirements.orig"):
            print("Restoring original requirements.txt file")
            commands = [
                "rm requirements.txt",
                "mv requirements.orig requirements.txt"
            ]
            self.run_os_commands(commands)

    def __create_softlink(self, lambda_func):
        """
        Softlink the lambda to lambda_function.py

        :param lambda_func: The path to the lambda function to softlink
        :return:
        """
        dir = os.path.dirname(lambda_func)
        aws_lambda_file_path = os.path.join(dir, 'lambda_function.py')

        # Clear out the symbolic link if it exists
        if os.path.exists(aws_lambda_file_path) and \
                os.path.islink(aws_lambda_file_path):
            print("Unlinking existing symbolic link: {}".format(
                aws_lambda_file_path))
            os.unlink(aws_lambda_file_path)

        print("Softlinking {} to {}".format(lambda_func,
                                            aws_lambda_file_path))
        os.symlink(lambda_func, aws_lambda_file_path)

    def run(self):
        commands = []
        commands.extend([
            "rm -rf {workspace}/{dir}".format(workspace=self.workspace,
                                              dir=WHEELHOUSE),
            "rm -rf {workspace}/{dir}".format(workspace=self.workspace,
                                              dir=DIST),
            "mkdir -p {workspace}/{dir}".format(workspace=self.workspace,
                                                dir=WHEELHOUSE),
            "mkdir -p {workspace}/{dir}".format(workspace=self.workspace,
                                                dir=DIST),
            "pip wheel --wheel-dir={workspace}/{dir} "
            "-r requirements.txt".format(workspace=self.workspace,
                                         dir=WHEELHOUSE),
            "unzip \"{workspace}/{dir}/*.whl\" -d {workspace}/{dir}".format(
                workspace=self.workspace, dir=WHEELHOUSE),
            "find {workspace}/{dir} -type f -name \"*.whl\" -delete".format(
                workspace=self.workspace, dir=WHEELHOUSE)
        ])

        print("Packing requirements.txt into wheelhouse")
        self.run_os_commands(commands)
        print("Generating local requirements.txt")
        self.localize_requirements()
        self.__create_softlink(self.lambda_func)
        print("Packing code and wheelhouse into dist")
        lambda_package = "{}/{}/{}-{}.zip".format(self.workspace, DIST,
                                                  self.ARCHIVE_NAME,
                                                  self.version)
        self.execute(
            "zip -9 {package_name} {files}".format(
                package_name=lambda_package,
                files=' '.join(glob.glob("lambda_function.py"))))
        os.chdir(os.path.join(self.workspace, WHEELHOUSE))
        self.execute(
            "zip -rg ../{dist}/{archive_name}-{version}.zip "
            "{files}".format(dist=DIST, archive_name=self.ARCHIVE_NAME,
                             version=self.version,
                             files=' '.join(glob.glob("**", recursive=True))))
        print("Copying {} to {}".format(os.path.abspath(lambda_package),
                                        self.package_dir))
        if not os.path.exists(self.package_dir):
            os.mkdir(self.package_dir)
        shutil.copyfile(os.path.abspath(lambda_package),
                        os.path.join(self.package_dir,
                                     os.path.basename(lambda_package)))
        os.chdir("../..")
        self.restore_requirements_txt()


setuptools.setup(
    name="lambda-event-misfire-handler",
    author="PCM",
    description="Lambda package for handling Event Misfire",
    packages=setuptools.find_packages(),
    include_package_data=True,
    cmdclass={
        "package": Package
    },
)
