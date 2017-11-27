# My DB design
Please import mydb to use our self-designed database, you can review the test1.py to 
learn how to use our API

* [DB Configuration](#Database-configuration)
* [Limitation of our database](#Limitation-of-our-database)
* [Execution requirement](#Execution-requirement)



## DB Configuration
### Adjust the memtable size and sstable list size

Please open the mydb.py and modify the `mem_size` and `lv0_size`.
I use the toy database to help me debug, so the default setting of mem_size and lv0_size is small

## Limitation of our database
Please remember our column-oriented database is composed of string as a key and dictionary as value.
Do not use other type to test our db, it can't work

## Execution requirement
Our database only can run at Python3(I use python3.4)

