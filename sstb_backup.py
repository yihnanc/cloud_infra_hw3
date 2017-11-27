from memtb import Memtable
from bloom_filter import BloomFilter
import csv
import os
table_size = 10;

class SSTable:
    def __init__(self, memtable, level, id):
        if not isinstance(memtable, Memtable):
            print("Should use memtable to initialize the sstable")
            self.mem = None
        else:
            self.mem = memtable

        self.bloom = BloomFilter()
        self.data_name = 'data_' + str(level) + '_' + str(id) + '.csv'
        self.index_name = 'index_' + str(level) + '_' + str(id) + '.csv'
        self.file  = open(self.data_name, 'w+')
        self.index = open(self.index_name, 'w+') 
        self.dataid = {}
        self.size = memtable.size()
        self.min = ""
        self.max = ""
        length = 0
        length_key = ""
        #print("fucking length:%d" %(memtable.size()))
        header = []
        for key, values in self.mem:
            tmp = values.keys()
            if len(tmp) > length:
                length = len(tmp)
                header = list(tmp);
                length_key = key

        #record the place of each column
        count = 0
        for val in header:
            self.dataid[val] = count
            count = count + 1

        cursor = csv.writer(self.file)
        index_cursor = csv.writer(self.index)

        #Write csv header
        cursor.writerow(header)
        for key, values in self.mem:
            lst = [""] * length
            self.bloom.add(key)
            # put the data to the corresponding column
            for kv in values.keys():
                lst[self.dataid[kv]] = values[kv]
            #Record the offset of data block
            file_pos = []
            file_pos.append(key)
            file_pos.append(str(self.file.tell()))

            if self.min == "":
                self.min = key
                self.max = key
            else:
                if (key < self.min):
                    self.min = key
                elif (key > self.max):
                    self.max = key

            index_cursor.writerow(file_pos)
            cursor.writerow(lst)

        self.file.close()
        self.index.close()
        test = open(self.data_name, 'r') 

    def merge(self, merge_table):
        #print("min:%s, max:%s" %(self.min, self.max))
        set1 = set()
        with open(merge_table.index_name, 'r') as file:
            lines = [line.strip() for line in file]

        merge_file = open(self.data_name, 'r+')
        append = []
        for line in lines:
            #Get the file offset of each key
            key = line.split(',')[0]
            updated_idx = int(line.split(',')[1])
            if (key in self.bloom):
                set1.add(key)
                input_file = open(merge_table.data_name, 'r')
                #label name of merged file
                label = input_file.readline()
                label_arr = label.rstrip().split(',')
                lst = [""] * len(label_arr)
                input_file.seek(updated_idx)
                updated_value = input_file.readline()
                updated_arr = updated_value.rstrip().split(',')
                #put the merged data into corresponding label(the place of the label of two sstable may be different)
                for i in range(len(label_arr)):
                    lst[self.dataid[label_arr[i]]] = updated_arr[i]
                idx = self.findIdx(self.index, key)
                merge_file.seek(idx)
                print(key)
                csvwt = csv.writer(merge_file) 
                csvwt.writerow(lst)
                input_file.close()
                
        return set1

    def update(self, allset):
        keyset = []
        valueset = []
        for key, values in self.mem:
            if (keys not in allset):
                keyset.add(key)
                valueset.add(values)
        return keyset, valueset

    def append(self, keyset, valueset):

        file  = open(self.data_name, 'r')
        length = len(file.readline().split(','))
        file.close()

        for i in range(len(valueset)):
            for ky in valueset[i].keys():
                if (self.dataid.get(ky) == None):
                    return False

        for i in range(len(keyset)):
            lst = [""] * length
            #column name is valueset
            for ky in valueset[i].keys():
                lst[self.dataid[ky]] = valueset[i][ky]

            file  = open(self.data_name, 'a')
            index = open(self.index_name, 'a') 

            pos = file.tell()
            data_csvwt = csv.writer(file) 
            data_csvwt.writerow(lst)

            #Write index
            index_row = []
            index_row.append(keyset[i])
            index_row.append(str(pos))
            index_csvwt = csv.writer(index) 
            index_csvwt.writerow(index_row)

            file.close()
            index.close()

        self.size = self.size + len(keyset)
        return True

    def findIdx(self, file, key):
        fd = open(self.index_name, 'r')
        lst = [line.rstrip('\n') for line in fd]
        start = 0
        end = len(lst) - 1
        while (start <= end):
            mid = start + int((end - start) / 2)
            arr = lst[mid].split(',')
            if (arr[0] == key):
                fd.close()
                return int(arr[1])
            elif (arr[0] < key):
                start = mid + 1
            else:
                end = mid - 1
    def getKeyRangeg(self):
        return self.min, self.max
