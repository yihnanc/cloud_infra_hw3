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
        self.id = id
        self.file  = open(self.data_name, 'w+')
        self.index = open(self.index_name, 'w+') 
        self.dataid = {}
        self.size = memtable.size()
        self.min = ""
        self.max = ""
        #print("fucking length:%d" %(memtable.size()))
        header = set()
        for key, values in self.mem:
            tmp = values.keys()
            header = header | set(values.keys()) 
        
        # print(header)
        length = len(header)
        #record the place of each column
        count = 0
        for val in header:
            self.dataid[val] = count
            count = count + 1

        cursor = csv.writer(self.file)
        index_cursor = csv.writer(self.index)

        #Write csv header
        cursor.writerow(list(header))
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

    def checkExist(self, merge_table):
        if (self.min > merge_table.max or self.max < merge_table.min):
            return False
        with open(merge_table.index_name, 'r') as file:
            input_lines = [line.strip() for line in file]
        for input_line in input_lines:
            key = input_line.split(',')[0]
            # print(key)
            if (key in self.bloom):
                return True;
        return False

    def merge(self, merge_table):

        new_file  = open("tmp_file", 'w+')
        new_index = open("tmp_index", 'w+') 
        new_bloom = BloomFilter()

        self.max = merge_table.max if merge_table.max > self.max else self.max
        self.min = merge_table.min if merge_table.min < self.min else self.min

        cursor = csv.writer(new_file)
        index_cursor = csv.writer(new_index)

        allset = set() 

        input_file = open(merge_table.data_name, 'r')
        input_label = input_file.readline()
        input_label_arr = input_label.rstrip().split(',')
        allset |= set(input_label_arr)
        input_file.close()

        merged_file = open(self.data_name, 'r')
        merged_label = merged_file.readline()
        merged_label_arr = merged_label.rstrip().split(',')
        allset |= set(merged_label_arr)
        merged_file.close()

        count = 0
        header = []
        for val in allset:
            self.dataid[val] = count
            header.append(val)
            count = count + 1

        cursor.writerow(header)

        with open(merge_table.index_name, 'r') as file:
            input_lines = [line.strip() for line in file]

        with open(self.index_name, 'r') as file:
            merged_lines = [line.strip() for line in file]

        i = 0
        j = 0
        self.size = 0
        while (i < len(input_lines) and j < len(merged_lines)):
            #index 0 is key, index 1 is offset
            input_line = input_lines[i].split(',')
            merged_line = merged_lines[j].split(',')

            #index 0 is key, index 1 is offset
            input_file = open(merge_table.data_name, 'r')
            input_label = input_file.readline()
            input_label_arr = input_label.rstrip().split(',')
            input_lst = [""] * len(allset)
            input_file.seek(int(input_line[1]))
            input_value = input_file.readline()

            # Avoid merging deleted files
            if ("," not in input_value):
                i = i + 1
                input_file.close()
                continue

            input_arr = input_value.rstrip().split(',')

            #index 0 is key, index 1 is offset
            merged_file = open(self.data_name, 'r')
            merged_label = merged_file.readline()
            merged_label_arr = merged_label.rstrip().split(',')
            merged_lst = [""] * len(allset)
            merged_file.seek(int(merged_line[1]))
            merged_value = merged_file.readline()

            # Avoid merging deleted files
            if ("," not in merged_value):
                j = j + 1
                merged_file.close()
                continue

            merged_arr = merged_value.rstrip().split(',')

           #print("shit:%s" %(merged_line[0]))
            
            if (input_line[0] <= merged_line[0]):
                for k in range(len(input_label_arr)):
                    input_lst[self.dataid[input_label_arr[k]]] = input_arr[k]
                pos = new_file.tell()
                cursor.writerow(input_lst)
                file_pos = []
                file_pos.append(input_line[0])
                file_pos.append(pos)
                index_cursor.writerow(file_pos) 
                new_bloom.add(input_line[0])
                self.size = self.size + 1
                if (input_line[0] == merged_line[0]):
                    #print("sdfgsdfg:%s, i:%d" %(input_line[0], i))
                    j = j + 1
                i = i + 1
            elif (input_line[0] > merged_line[0]):
                for k in range(len(merged_label_arr)):
                    merged_lst[self.dataid[merged_label_arr[k]]] = merged_arr[k]
                pos = new_file.tell()
                cursor.writerow(merged_lst)
                file_pos = []
                file_pos.append(merged_line[0])
                file_pos.append(pos)
                index_cursor.writerow(file_pos) 
                new_bloom.add(merged_line[0])
                self.size = self.size + 1
                j = j + 1
            input_file.close()
            merged_file.close()

        while (i < len(input_lines)):
            input_line = input_lines[i].split(',')
            #index 0 is key, index 1 is offset
            input_file = open(merge_table.data_name, 'r')
            input_label = input_file.readline()
            input_label_arr = input_label.rstrip().split(',')
            input_lst = [""] * len(allset)
            input_file.seek(int(input_line[1]))
            input_value = input_file.readline()

            # Avoid merging deleted files
            if ("," not in input_value):
                i = i + 1
                input_file.close()
                continue

            new_bloom.add(input_line[0])
            input_arr = input_value.rstrip().split(',')
            for k in range(len(input_label_arr)):
                input_lst[self.dataid[input_label_arr[k]]] = input_arr[k]
            pos = new_file.tell()
            cursor.writerow(input_lst)
            file_pos = []
            file_pos.append(input_line[0])
            file_pos.append(pos)
            index_cursor.writerow(file_pos) 
            i = i + 1
            self.size = self.size + 1
            input_file.close()

        while (j < len(merged_lines)):
            merged_line = merged_lines[j].split(',')
            #index 0 is key, index 1 is offset
            merged_file = open(self.data_name, 'r')
            merged_label = merged_file.readline()
            merged_label_arr = merged_label.rstrip().split(',')
            merged_lst = [""] * len(allset)
            merged_file.seek(int(merged_line[1]))
            merged_value = merged_file.readline()

            # Avoid merging deleted files
            if ("," not in merged_value):
                j = j + 1
                merged_file.close()
                continue

            new_bloom.add(merged_line[0])
            merged_arr = merged_value.rstrip().split(',')
            for k in range(len(merged_label_arr)):
                merged_lst[self.dataid[merged_label_arr[k]]] = merged_arr[k]
            pos = new_file.tell()
            cursor.writerow(merged_lst)
            file_pos = []
            file_pos.append(merged_line[0])
            file_pos.append(pos)
            index_cursor.writerow(file_pos) 
            j = j + 1
            self.size = self.size + 1
            merged_file.close()

        
        new_file.close()
        new_index.close()
        os.remove(self.data_name)
        os.remove(self.index_name)
        os.remove(merge_table.data_name)
        os.remove(merge_table.index_name)
        self.file = new_file
        self.index = new_index
        self.bloom = new_bloom

        os.rename("tmp_file", self.data_name)
        os.rename("tmp_index", self.index_name)

    def markDelete(self, key):
        if (key not in self.bloom):
            return

        fd = open(self.index_name, 'r')
        lst = [line.rstrip('\n') for line in fd]
        start = 0
        end = len(lst) - 1
        while (start <= end):
            mid = start + int((end - start) / 2)
            arr = lst[mid].split(',')
            if (arr[0] == key):
                fd.close()
                ret_dict = {}
                fd = open(self.data_name, 'r+')
                columns = fd.readline().rstrip('\n').split(',')
                fd.seek(int(arr[1]))
                delete = fd.readline()
                print("%d, %s" %(len(delete),delete))
                fd.seek(int(arr[1]))
                replace = "X" * (len(delete) - 1)
                fd.write(replace)
                fd.close()
                break
            elif (arr[0] < key):
                start = mid + 1
            else:
                end = mid - 1

    def searchKey(self, key):
        if (key not in self.bloom):
            return None

        fd = open(self.index_name, 'r')
        lst = [line.rstrip('\n') for line in fd]
        start = 0
        end = len(lst) - 1
        while (start <= end):
            mid = start + int((end - start) / 2)
            arr = lst[mid].split(',')
            if (arr[0] == key):
                fd.close()
                ret_dict = {}
                fd = open(self.data_name, 'r')
                columns = fd.readline().rstrip('\n').split(',')
                fd.seek(int(arr[1]))
                query_value = fd.readline()
                values = query_value.rstrip('\n').split(',')
                if ("," not in query_value):
                    return None
                for i in range(len(columns)):
                    ret_dict[columns[i]] = values[i]
                return ret_dict
            elif (arr[0] < key):
                start = mid + 1
            else:
                end = mid - 1

        return None
        

""" unused code
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
"""
