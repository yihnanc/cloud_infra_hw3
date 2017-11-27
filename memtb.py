import sys
sys.path.append("pyskip/")
from pyskip import Skiplist

class Memtable:
    def __init__(self):
        self.mutable = True
        self.skiplist = Skiplist()
        self.columns = {}

    def insertMultiple(self, keyset, valueset):
        for i in range(len(keyset)):
            self.insert(keyset[i], valueset[i])

    def remove(self, key):
        self.skiplist.remove(key)
        del self.columns[key]

    def insert(self, key, value):
        if not isinstance(value, dict) or not isinstance(key, str):
            print("The key should be string, the value should be dict")
            return

        if key not in self.skiplist:
            self.skiplist.insert(key)
        self.columns[key] = value

    def __contains__(self, key):
        return key in self.skiplist

    def __iter__(self):
        self.it = iter(self.skiplist)
        return self

    def __next__(self):
        key = str(next(self.it))
        return key, self.columns[key]
            
    def size(self):
        return len(self.skiplist)
