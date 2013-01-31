class FileSystemServiceBase(object):

    def __init__(self, *args, **kwargs):
        self.context = kwargs.get("config")
