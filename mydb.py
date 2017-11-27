from memtb import Memtable                                                                                                                                                                              
from sstb import SSTable
import os
import random

mem_size = 3
lv0_size = 3

class DB:
    def __init__(self):
        self.memtb = Memtable()
        self.immutable = None
        self.lv0 = []
        self.lv1 = []
        self.dict0 = {}
        self.dict1 = {}
        self.setlv0 = set()
        self.setlv1 = set()

    def insert(self, key, value):
        if (self.memtb.size() == mem_size):
            if (self.immutable == None):
                self.immutable = self.memtb
                self.memtb = Memtable()
            else:
                #Can be done by multithread
                #merge immutable to lv0
                id = random.randrange(0, lv0_size)
                while (id in self.setlv0):
                    id = random.randrange(0, lv0_size)
                newsstb = SSTable(self.immutable, 0, id)
                self.immutable = self.memtb
                self.memtb = Memtable()
                self.lv0 = self.compact(self.lv0, newsstb, 0, self.setlv0, lv0_size)
                # Find how many keys in lv0  also exist in lv1
                # .....
                # Merge
                # merge lv0 to lv1, delete the file in lv0
                # print("lv0 size:%d" %(len(self.lv0)))
                if (len(self.lv0) == lv0_size):
                    poptb = self.lv0.pop(0)
                    self.lv1 = self.compact(self.lv1, poptb, 1, self.setlv1, 100)
                    self.setlv0.remove(poptb.id)
                    # print(self.setlv0)

        self.memtb.insert(key, value)

    def remove(self, key):
        if (key in self.memtb):
            self.memtb.remove(key)

        # print(self.immutable.columns)
        if (key in self.immutable):
            self.immutable.remove(key)

        for sstb in self.lv0:
            columns = sstb.markDelete(key)
            if (columns is not None):
                return columns

        for sstb in self.lv1:
            columns = sstb.markDelete(key)
            if (columns is not None):
                return columns

    def query(self, key):
        if (key in self.memtb):
            return self.memtb.columns[key]

        if (key in self.immutable):
            return self.immutable.columns[key]

        for sstb in self.lv0:
            columns = sstb.searchKey(key)
            if (columns is not None):
                return columns

        for sstb in self.lv1:
            columns = sstb.searchKey(key)
            if (columns is not None):
                return columns

        return None

    def compact(self, lst, merged_table, level, id_set, level_size):
        exist = []
        tmp = []
        if (len(lst) == 0):
            data_name = 'data_' + str(level) + '_0' + '.csv' 
            index_name = 'index_' + str(level) + '_0' + '.csv' 
            os.rename(merged_table.data_name, data_name)
            os.rename(merged_table.index_name, index_name)
            merged_table.index_name = index_name
            merged_table.data_name = data_name
            merged_table.id = 0
            id_set.add(0)
            tmp.append(merged_table)
        else:
            for i in range(len(lst) - 1, -1, -1):
                current = lst[i]
                if (current.checkExist(merged_table)):
                    exist.append(current)
                else:
                    tmp.insert(0, current)
            if (len(exist) == 0):
                while (merged_table.id in id_set):
                    merged_table.id = random.randrange(0, level_size)
                id_set.add(merged_table.id)
                data_name = 'data_' + str(level) + '_' + str(merged_table.id) + '.csv' 
                index_name = 'index_' + str(level) + '_' + str(merged_table.id) + '.csv' 
                os.rename(merged_table.data_name, data_name)
                os.rename(merged_table.index_name, index_name)
                tmp.append(merged_table)
            else:
                merged = merged_table
                # print("idset:%s, name:%s" %(id_set, merged_table.data_name))
                for sstb in exist:
                    if (merged != merged_table):
                        id_set.remove(merged.id)
                    sstb.merge(merged)
                    merged = sstb
                tmp.append(merged)

        return tmp

