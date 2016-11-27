#!/usr/bin/env python
import threading

class LinkedListElement():
    """ The element of a linked list."""
    def __init__(self, value, next=None):
        self.value = value
        if next:
            self.append(next)
        else:
            self.next = None
            self.prev = None
    def delete(self):
        self.prev.next = self.next
        self.next.prev = self.prev
        return self.value
    def append(self, next):
        self.prev = next.prev
        self.next = next
        next.prev.next = self
        next.prev = self


class LinkedList():
    """ A linked list that is used by yas3fs as a LRU index
    for the file system cache."""
    def __init__(self):
        self.tail = LinkedListElement(None)
        self.head = LinkedListElement(None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.index = {}
        self.lock = threading.RLock()
    def append(self, value):
        with self.lock:
            if value not in self.index:
                new = LinkedListElement(value, self.tail)
                self.index[value] = new
    def popleft(self):
        with self.lock:
            if self.head.next != self.tail:
                value = self.head.next.delete()
                del self.index[value]
                return value
            else:
                return None
    def delete(self, value):
        with self.lock:
            if value in self.index:
                self.index[value].delete()
                del self.index[value]
    def move_to_the_tail(self, value):
        with self.lock:
            if value in self.index:
                old = self.index[value]
                old.delete()
                old.append(self.tail)
