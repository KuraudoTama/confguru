from confguru.backends import ZKClient


class Guru(object):
    """A Common Client to Interact with Various Configuration Endpoints
    """

    def __init__(self, backend_type, *args, **kwargs):
        if backend_type == "zookeeper":
            self = ZKClient(*args, **kwargs)
