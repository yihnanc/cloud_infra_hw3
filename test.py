from memtb import Memtable
from sstb import SSTable
from pyskip import Skiplist

print("fuck")
mine = Memtable()
mine1 = Memtable()
print(mine.size())
#mine.insert("5",{"1":"asdfg", "324":"qqqq", "erersfdg":"t55ttt"})
mine.insert("5",{"1":"rwetwert"})
mine.insert("3",{"1":"eeee"})
mine.insert("10",{"324":"524"})
mine.insert("12",{"324":"624"})

mine1.insert("4",{"1":"sdfgsdfg", "324":"rrrrr", "erersfdg":"tttttt"})
mine1.insert("5",{"1":"rwetwert"})
mine1.insert("7",{"erersfdg":"ttrsdfsdfg"})
mine1.insert("10",{"324":"424"})
mine.insert("14",{"1":"fucksdfgsdfg", "324":"jjrrr", "erersfdg":"tttttt"})

skip = Skiplist()
skip.insert("gdfgs")
skip.insert("cccc")
it = iter(skip)
ff = iter(mine)
print(next(ff))
print(next(ff))
sss = SSTable(mine, 0, 0)
ssss = SSTable(mine1, 1, 0)
print(ssss.merge(sss))
keyset = ["89", "99"]
valueset = [{"1":"erer", "324":"wewew"}, {"1":"peipei", "324":"yorkyork"}]
with open('index_1_0.csv', 'r') as file:
    input = [line.strip() for line in file]
for val in input:
    keys = val.split(',')
    file = open('data_1_0.csv', 'r')
    file.seek(int(keys[1]))
    print(file.readline())
    file.close()
print(input)
#ssss.append(keyset, valueset)
