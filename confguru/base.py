import abc


class BaseConf(object):
    """Basic Class for all Backend Endpoint Classes
    """

    __metaclass__ = abc.ABCMeta

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__,
                            str(self))

    @abc.abstractmethod
    def __str__(self):
        pass

    def __eq__(self, other):
        """
        identify whether the other one represents a connection to a backend
        """

        if not isinstance(other, self.__class__):
            return False
        if not other.url == self.url:
            return False
        return True

    def getattr(self, attr):
        try:
            return self.__getattribute__(attr)
        except:
            return None

    def __getitem__(self, key):
        return self.__getattribute__(key)

    @abc.abstractmethod
    def get(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def create(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def delete(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def update(self, *args, **kwargs):
        pass
