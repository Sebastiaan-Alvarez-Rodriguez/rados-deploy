import os


class Environment(object):
    '''Class to load and store persistent variables in a way that does not depend on OS environment vars, login shells, shell types, etc.'''
    def __init__(self):
        self._entered = False

        self._path = Environment.get_path()
        os.makedirs(Environment.get_storedir(), exist_ok=True)
        
        import configparser
        self.parser = configparser.ConfigParser()
        self.parser.optionxform=str
        if os.path.isfile(self._path):
            self.parser.read(self._path)

    @staticmethod
    def get_path():
        return os.path.join(Environment.get_storedir(), 'env.cfg')

    @staticmethod
    def get_storedir():
        return os.path.join(os.getenv('HOME'), '.rados_deploy')


    def get(self, key):
        '''Getter, different from "env[key]"" in that it does not throw.
        Returns:
            Found value on success, `None` otherwise.'''
        return self.parser['DEFAULT'][key] if key in self.parser['DEFAULT'] else None

    def set(self, key, value):
        '''Function to add a single key-valuepair. Note: For setting multiple keys, use a "with env:" block, followed by "env[key] = value" or "env.set(key, value)".'''
        self.parser['DEFAULT'][key] = value
        os.environ[key]= value
        if not self._entered:
            self.persist()

    def load_to_env(self):
        '''Loads all stored variables into the process environment.'''
        for key, value in self.parser['DEFAULT'].items():
            os.environ[key] = value

    def persist(self):
        with open(self._path, 'w') as file:
            self.parser.write(file)


    def __enter__(self):
        self._entered = True
        return self


    def __getitem__(self, key):
        return self.parser['DEFAULT'][key]


    def __setitem__(self, key, value):
        if not self._entered:
            raise NotImplementedError('Cannot directly set Environment variables. Use "env.set()", or "with env:"')
        else:
            os.environ[key]= value
            self.parser['DEFAULT'][key] = value


    def __exit__(self, exc_type, exc_value, traceback):
        self.persist()
        self._entered = False