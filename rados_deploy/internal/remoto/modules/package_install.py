

def remote_pip_install_simple(name, silent):
    return remote_pip_install(name, True, 'python3', 'pip3', silent)

def remote_pip_install(name, usermode, py, pip, silent):
    return lib_install(name, usermode=usermode, py=py, pip=pip, silent=silent) 
