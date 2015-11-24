from oslo_utils import importutils
from exceptions import NotImplementedError


BACKENDS_TYPE = {"zookeeper": "ZKClient",
                 "json": "JsonBe",
                 "yml": "YmlBe"}


def Guru(backend_type, *args, **kwargs):
    """Initialize client object based on given backend type.

    HOW-TO:
    The simplest way to create a client instance is initialization with your
    credentials::

        >>> from confguru import guru
        >>> zookeeper = guru.Guru(backend_type, hosts,
        ...                       auth_data=None, timeout=10)

    Here ``backend_type`` is a string.

    Current supported backend types are 'zookeeper', 'json' and 'yml'.
    """

    client_class = _get_backend_class(backend_type)
    return client_class(*args, **kwargs)


def _get_backend_class(backend_type):
    if backend_type not in BACKENDS_TYPE:
        raise NotImplementedError("%s is not supported now. "
                                  "Please select one "
                                  "from %s" % (backend_type,
                                               BACKENDS_TYPE.keys()))

    return importutils.import_class(
        "confguru.backends.%s" % BACKENDS_TYPE[backend_type])
