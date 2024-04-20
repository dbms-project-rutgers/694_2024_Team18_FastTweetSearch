from collections import OrderedDict


class _TTLLink:
    """Link in TTL Doubly Linked List.

    Attributes:
        key (hashable): Cache Item Key.
        expiry (int): Item Expiry Time.
        next (_TTLLink): Next Link in the DLL.
        prev (_TTLLink): Prev Link in the DLL. None
        if no previous link exists.
    """
    def __init__(self, key=None, expiry=None, nxt=None, prev=None):
        self.key = key
        self.expiry = expiry
        self.next = nxt
        self.prev = prev


class _TTLLinkedList:
    """Doubly Linked List of TTL Links.

    Attributes:
        head (_TTLLink): Head of the Linked List.
    """
    def __init__(self, head=None) -> None:
        self.__head = head
        # 'TTLCache' only inserts at the end of the
        # list. Reference to the tail of the list
        # for O(1) insertions.
        self.__tail = head

    @property
    def head(self):
        """Returns the head of the linked list."""
        return self.__head

    def insert(self, link):
        """Insert a new link at the end of the linked list.

        Args:
            link (_TTLLink): `_TTLLink` to insert.
        """
        if self.__head:
            link.prev = self.__tail
            link.prev.next = self.__tail = link
        else:
            self.__head = self.__tail = link

    def remove(self, link):
        """Remove a link from the linked list.

        Args:
            link (_TTLLink): `_TTLLink` to remove.
        """
        if self.__head == link:
            self.__head = self.__head.next
        elif self.__tail == link:
            self.__tail = self.__tail.prev
        else:
            link.prev.next = link.next
            link.next.prev = link.prev


