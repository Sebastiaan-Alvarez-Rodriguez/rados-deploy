import distutils.sysconfig as sysconfig
import itertools
import re
import sys
import types

import rados_deploy.internal.util.fs as fs
from rados_deploy.internal.util.printer import *


def _generate_stl_libs():
    '''Generator for stl-names. Does a breadth-first search on the python installation directory. Adds built-in library names.
    Returns:
        `set(str)` containing all known standard-library files.'''
    ret_list = set()
    std_lib = sysconfig.get_python_lib(standard_lib=True)
    std_lib_len = len(std_lib)

    found = set()
    sep = fs.sep()

    to_visit = list()
    to_visit.append(std_lib)
    while any(to_visit):
        visit_now = to_visit.pop()
        to_visit += list(fs.ls(visit_now, only_dirs=True, full_paths=True))

        files = list(fs.ls(visit_now, only_files=True, full_paths=True))
        found.update(set('.'.join(y[std_lib_len+1:].split(sep))[:-3] for y in files if y[-3:] == '.py' and y[-11:-3] != '__init__'))
        if any(True for x in files if x[-11:] == '__init__.py') and visit_now != std_lib: #If we found '/path/to/python_lib/oof/a/__init__.py', then assume library 'oof.a' exists.
            found.add('.'.join(visit_now[std_lib_len+1:].split(sep)))
    found.update(set(sys.builtin_module_names))
    return found


class ModuleGenerator(object):
    '''Object to quickly construct self-contained modules, for use with remoto.
    Warning: We have several constraints for the input modules/files:
        1. When including a module X which internally imports module Y, Y is provided to the generator too. Failing to do so will produce a faulty module.
        2. When including a module X and a module Y, there are no name conflicts between X and Y.
        3. For all modules X, X does not have statements that import user-provided modules/files using import statements "import A as B", or "from A import a as b".
        4. All uses of user-provided modules/files must be as if the user-provided modules.'''
    def __init__(self):
        self._files = []
        self._stl_modules_cache = []

    def with_module(self, module):
        if not isinstance(module, types.ModuleType):
            raise ValueError('Require a module!')
        if self._is_regular_python(module.__name__):
            raise ValueError('We do not export stl modules. Given stl module: "{}"'.format(module.__name__))
        self._files.append(module.__file__)
        return self

    def with_modules(self, *modules):
        for x in modules:
            self.with_module(x)
        return self

    def with_file(self, filepath):
        if not fs.isfile(filepath):
            raise ValueError('Path "{}" does not refer to a file.'.format(filepath))
        self._files.append(str(filepath))
        return self

    def with_files(self, *filepaths):
        for x in filepaths:
            self.with_file(x)
        return self

    def _is_regular_python(self, name):
        if not self._stl_modules_cache:
            self._stl_modules_cache = list(_generate_stl_libs())
            self._stl_modules_cache.append('time') # so we are able to import time
        return name in self._stl_modules_cache


    def _read_imports(self, allowed_imports=None, silent=False):
        '''Reads imports from all files. Non-stl python libraries are skipped, except those specifically allowed in `allowed_imports`.
        Args:
            allowed_imports (optional iterable(str)): If set to an iterable, does not remove given import statements.
            silent (optional bool): If set, prints warnings about found non-stl python libraries.

        Returns:
            `(set, set)`: The first set contains all found stl import names using format 'import x', with elements `x`.
                          The second set contains all found stl import names using format 'from x import y (as z)', with elements `(x, y)` and `(x, y, z)`.'''
        regex_import = re.compile(r'^ *import +([a-zA-Z\._0-9]+)', re.MULTILINE)
        regex_from_import = re.compile(r'^ *from +([a-zA-Z\._0-9]+) import +((?:[a-zA-Z\._0-9]+, *)*[a-zA-Z\._0-9]+) *(?:as)? *([a-zA-Z\._0-9]+)?', re.MULTILINE)
        found_stl_imports = set()
        found_stl_import_froms = set()

        allowed_set = set(allowed_imports) if allowed_imports else None

        for x in self._files:
            with open(x, 'r') as f:
                lines = f.read()

                for match in itertools.chain(regex_import.finditer(lines), regex_from_import.finditer(lines)):
                    matchtuple = match.groups()
                    match_importmodule = matchtuple[0]

                    if (not self._is_regular_python(match_importmodule)) and not (allowed_set and match_importmodule in allowed_set):
                        if not silent:
                            printw('(file: {}) Found non-regular import "{}".'.format(x, match_importmodule))
                    else:
                        if len(matchtuple) > 1:
                            found_stl_import_froms.add(matchtuple)
                        else:
                            found_stl_imports.add(matchtuple[0])
        if not any(found_stl_import_froms):
            raise ValueError('Empty from sequence.')
        return found_stl_imports, found_stl_import_froms


    def _read_non_imports(self, filepath):
        '''Reads given `filepath`, strips lines containing Python import statements (careful with comments, don't use multiline statements with ';'), returns as generator.'''
        regex_no_import = re.compile(r'^((?!(?:from .+)? *import +.*).)+$', re.MULTILINE)
        # ^((?!(?:from .+)? *import +(?:.*) *(?:as .*)?).)+$
        # 
        with open(filepath, 'r') as f:
            lines = f.read()
            return '\n'.join(x.group(0) for x in regex_no_import.finditer(lines))


    def generate(self, outputpath, allowed_imports=None, silent=False):
        '''Generates the final, non-stl dependency-free module to be used with Remoto remote module execution. 
        Captures all import commands and ensures they are present only once for the entire module.
        Warning: Removes all non-stl import statements.
        Args:
            outputpath (str): Location to store module, including output filename. Creates every directory  that does not exist.
            allowed_imports (optional iterable(str)): If set to an iterable, does not remove given import statements.
            silent (optional bool): If set, skips printing warnings when non-standard imports are encountered.'''
        dest_dir = fs.dirname(outputpath)
        if not fs.isdir(dest_dir):
            fs.mkdir(dest_dir, exist_ok=True)


        stl_imports, stl_imports_from = self._read_imports(allowed_imports=allowed_imports, silent=silent)
        with open(outputpath, 'w') as f:
            header = '''

################################################################################
# Generated by the meta modulegenerator
# Processed {} files/modules:
{}
################################################################################

'''.format(len(self._files), '\n'.join('#    {}'.format(x) for x in self._files))
            f.write(header)
            importstring = '\n'+'\n'.join('import {}'.format(name) for name in stl_imports)
            importstring += '\n'
            importstring += '\n'.join('from {} import {} as {}'.format(*names) if len(names) == 3 and names[2] != None else 'from {} import {}'.format(*names) for names in stl_imports_from)
            f.write(importstring)
            for x in self._files:
                content = self._read_non_imports(x)
                f.write('''
################################################################################
# Created from file {}
'''.format(x))
                f.write(content)
                f.write('''
################################################################################

''')