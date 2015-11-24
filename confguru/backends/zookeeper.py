from confguru.base import BaseConf
from kazoo.client import KazooClient
import json
import yaml
import logging
import six


class ZKClient(BaseConf):
    """A wrapped python client for zookeeper based on Kazoo

    :param hosts: Comma-separated list of hosts to connect to
        (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
    :param auth_data: A list of authentication credentials to use
        for the connection. Should be a list of (scheme, credential) tuples.
    :param timeout: The longest to wait for a Zookeeper connection
    """

    log = logging.getLogger("confguru.backends.ZKClient")

    def __init__(self, hosts,
                 auth_data=None,
                 timeout=10):

        self.hosts = hosts
        self.auth_data = auth_data
        self.timeout = timeout

        self.kazoo = KazooClient(hosts=self.hosts,
                                 timeout=timeout,
                                 auth_data=auth_data,
                                 logger=self.log)
        self.kazoo.start()

    def __str__(self):
        return "Hosts @ %s" % self.hosts

    def create(self, path, value=b"", acl=None,
               ephemeral=False, sequence=False, makepath=True):
        """Create a znode with the given value as its data. Optionally set an
        ACL on the znode.

        :param path: Path of znode.
        :param value: Initial bytes value of znode.
        :param acl: ACL list.
        :param ephemeral: Boolean indicating whether node is ephemeral (tied
            to this session).
        :param sequence: Boolean indicating whether path is suffixed with a
            unique index.
        :param makepath: Whether the path should be created if it
            doesn't exist.
        :returns: None
        """

        self.log.debug("Start to create znode %s", path)
        self.kazoo.create(path, value=bytes(value), acl=acl,
                          ephemeral=ephemeral, sequence=sequence,
                          makepath=makepath)

    def get(self, path, watch=None):
        """Get the value of a znode.

        If a watch is provided, it will be left on the znode with the given
        path. The watch will be triggered by a successful operation that sets
        data on the znode, or deletes the znode.

        :param path: Path of znode.
        :param watch: Optional watch callback to set for future changes to
            this path.
        :return: Tuple (value, ZnodeStat) of znode.
        :rtype: tuple
        :raises: `kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.
        """

        self.log.debug("Fetch data of znode %s", path)
        return self.kazoo.get(path, watch=watch)

    def delete(self, path, version=-1, recursive=False):
        """Delete a znode.

        The call will succeed if such a znode exists, and the given version
        matches the znode's version (if the given version is -1, the default,
        it matches any znode's versions).

        :param path: Path of znode.
        :param version: Version of node to delete, or -1 for any.
        :param recursive: Recursively delete node and all its children,
            defaults to False.
        :type recursive: bool
        :returns: None
        :raises:
            `kazoo.exceptions.BadVersionError` if version doesn't
            match.

            `kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            `kazoo.exceptions.NotEmptyError` if the node has
            children.

            `kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """

        self.log.debug("Start to delete znode %s with recursive=%s",
                       path, recursive)
        self.kazoo.delete(path, version=version, recursive=recursive)
        self.log.info("Successfully delete node %s with recursive=%s",
                      path, recursive)

    def update(self, path, value, version=-1):
        """Set the value of a znode.

        If the version of the znode being updated is newer than the supplied
        version (and the supplied version is not -1), a BadVersionError will
        be raised.

        The maximum allowable size of the value is 1 MB. Values larger
        than this will cause a ZookeeperError to be raised.

        :param path: Path of znode.
        :param value: New data value.
        :param version: Version of znode being updated, or -1.
        :returns: Updated :class:`kazoo.protocol.states.ZnodeStat` of
                  the node.
        :raises:
            `kazoo.exceptions.BadVersionError` if version doesn't
            match.

            `kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            `kazoo.exceptions.ZookeeperError` if the provided
            value is too large.

            `kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """

        self.log.debug("Start to update data of znode %s", path)
        return self.kazoo.set(path, value, version=version)

    @classmethod
    def make_digest_acl(cls, username, password, read=False, write=False,
                        create=False, delete=False, admin=False, all=False):
        """Create a digest ACL for Zookeeper with the given permissions
        for use with ZKClient's ACL methods

        :param username: Username to use for the ACL.
        :param password: A plain-text password to hash.
        :param write: Write permission.
        :type write: bool
        :param create: Create permission.
        :type create: bool
        :param delete: Delete permission.
        :type delete: bool
        :param admin: Admin permission.
        :type admin: bool
        :param all: All permissions.
        :type all: bool

        :rtype: :class:`kazoo.security.ACL`

        """

        from kazoo.security import make_digest_acl
        return make_digest_acl(username, password,
                               read=read, write=write, create=create,
                               delete=delete, admin=admin, all=all)

    @classmethod
    def make_acl(cls, scheme, credential, read=False, write=False,
                 create=False, delete=False, admin=False, all=False):
        """Given a scheme and credential, return an
        :class:`kazoo.security.ACL` object appropriate for use with ZKClient.

        :param scheme: The scheme to use. I.e. `digest`.
        :param credential:
            A colon separated username, password. The password should be
            hashed with the `scheme` specified. The
            :meth:`make_digest_acl_credential` method will create and
            return a credential appropriate for use with the `digest`
            scheme.
        :param write: Write permission.
        :type write: bool
        :param create: Create permission.
        :type create: bool
        :param delete: Delete permission.
        :type delete: bool
        :param admin: Admin permission.
        :type admin: bool
        :param all: All permissions.
        :type all: bool
        :rtype: :class:`kazoo.security.ACL`

        """

        from kazoo.security import make_acl
        return make_acl(scheme, credential,
                        read=read, write=write, create=create,
                        delete=delete, admin=admin, all=all)

    def initialize_from_json_file(self, init_json_file, acl=None,
                                  ephemeral=False, sequence=False,
                                  makepath=True):
        """Initialize the data from json file

        :param init_json_file: json file path
        :param acl: ACL list.
        :param ephemeral: Boolean indicating whether node is ephemeral (tied
            to this session).
        :param sequence: Boolean indicating whether path is suffixed with a
            unique index.
        :param makepath: Whether the path should be created if it
            doesn't exist.

        """

        with open(init_json_file) as json_file:
            json_data = json.load(json_file)
            self.log.debug("Successfully loads json data from file %s",
                           init_json_file)

        root_keys = json_data.keys()
        for root_key in root_keys:
            self._create_children(root_key, json_data[root_key],
                                  acl=acl, ephemeral=ephemeral,
                                  sequence=sequence, makepath=makepath)
        self.log.info("Successfully initialize the data")

    def initialize_from_yml_file(self, init_yml_file, acl=None,
                                 ephemeral=False, sequence=False,
                                 makepath=True):
        """Initialize the data from yml file

        :param init_yml_file: yml file path
        :param acl: ACL list.
        :param ephemeral: Boolean indicating whether node is ephemeral (tied
            to this session).
        :param sequence: Boolean indicating whether path is suffixed with a
            unique index.
        :param makepath: Whether the path should be created if it
            doesn't exist.

        """

        with open(init_yml_file) as yml_file:
            yml_data = yaml.load(yml_file)
            self.log.debug("Successfully loads yml data from file %s",
                           init_yml_file)

        root_keys = yml_data.keys()
        for root_key in root_keys:
            self._create_children(root_key, yml_data[root_key],
                                  acl=acl, ephemeral=ephemeral,
                                  sequence=sequence, makepath=makepath)
        self.log.info("Successfully initialize the data")

    def _create_children(self, path, value="", acl=None,
                         ephemeral=False, sequence=False, makepath=True):
        # empty list or ""
        if not value:
            self._create_child(path, value="", acl=acl,
                               ephemeral=ephemeral, sequence=sequence,
                               makepath=makepath)

        # string types
        if isinstance(value, six.string_types):
            self._create_child(path, value=value, acl=acl,
                               ephemeral=ephemeral, sequence=sequence,
                               makepath=makepath)

        # list
        if isinstance(value, list):
            for ele in value:
                self._create_children(path, ele, acl=acl,
                                      ephemeral=ephemeral, sequence=sequence,
                                      makepath=makepath)

        # dict
        if isinstance(value, dict):
            for (child_key, child_value) in value.iteritems():
                self._create_children("/".join([path, child_key]),
                                      child_value,
                                      acl=acl,
                                      ephemeral=ephemeral, sequence=sequence,
                                      makepath=makepath)

    def _create_child(self, path, value="", acl=None,
                      ephemeral=False, sequence=False, makepath=True):
        if not self.exists(path):
            self.create(path, value=bytes(value), acl=acl,
                        ephemeral=ephemeral, sequence=sequence,
                        makepath=makepath)
        else:
            self.update(path, bytes(value))

    def dump_to_json_file(self, dump_json_file, starting_root="/",
                          indent=None, ensure_ascii=True, separators=None,
                          encoding='utf-8', sort_keys=False):
        """
        Dump the data to json file

        To access the nodes with strict/specified credentials, you
        have to first provide authentication data in `auth_data` when
        initializing the client.

        :param dump_json_file: json file path
        :param starting_root: the start root path
        :param ensure_ascii:
            If `ensure_ascii` is True, all non-ASCII characters in the output
            are escaped with \uXXXX sequences, and the result is a str instance
            consisting of ASCII characters only.

            If `ensure_ascii` is False, some chunks written to `dump_json_file`
            may be unicode instances. This usually happens because the input
            contains unicode strings or the encoding parameter is used.
        :param indent:
            If `indent` is a non-negative integer, then JSON array elements
            and object members will be pretty-printed with that indent level.

            An indent level of 0, or negative, will only insert newlines.
            None (the default) selects the most compact representation.
        :param separators: the item separator
        :param encoding: the character encoding for str instances
        :param sort_keys: whether to sort the output of dictionaries by key.

        For other parameters, please refer to `json.dump`

        """

        with open(dump_json_file, 'w') as json_fp:
            self.log.debug("Start to dump data to json file %s",
                           dump_json_file)
            children_data = self._dump_children(starting_root)
            if children_data is None:
                self.log.debug("No data was found")
                return
            json.dump(children_data, json_fp,
                      indent=indent, ensure_ascii=ensure_ascii,
                      separators=separators, encoding=encoding,
                      sort_keys=sort_keys)

        self.log.info("Successfully dump data to json file %s",
                      dump_json_file)

    def dump_to_yml_file(self, dump_yml_file, starting_root="/"):
        """Dump the data to yml file

        To access the nodes with strict/specified credentials, you
        have to first provide authentication data in `auth_data` when
        initializing the client.

        :param dump_yml_file: yml file path
        :param starting_root: the start root path

        """

        with open(dump_yml_file, 'w') as yml_fp:
            self.log.debug("Start to dump data to yml file %s",
                           dump_yml_file)
            children_data = self._dump_children(starting_root)
            if children_data is None:
                self.log.debug("No data was found")
                return
            yaml.dump(children_data, yml_fp)

        self.log.info("Successfully dump data to yml file %s",
                      dump_yml_file)

    def _dump_children(self, path):
        list_flag = False

        cur_path = path.split("/")[-1]
        if not cur_path:
            cur_path = "/"

        data_tree = dict()

        # current node value
        try:
            self.log.debug("Try to access path %s", path)
            (cur_val, nodestat) = self.get(path)
        except:
            self.log.debug("You do not have the right to access path %s",
                           path)
            return None

        if cur_val:
            # valid string
            list_flag = True

        children = self.get_children(path)

        # have children
        if children:
            children_data = dict()
            if list_flag is True:
                data_tree[cur_path] = [cur_val]
                data_tree[cur_path].append(children_data)
            else:
                data_tree[cur_path] = children_data

            for child in children:
                tpm_data = self._dump_children("/".join([path,
                                                         child]))
                if tpm_data is None:
                    continue
                children_data.update(tpm_data)

        else:
            data_tree[cur_path] = cur_val

        return data_tree
