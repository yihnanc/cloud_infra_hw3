import mydb

def fuck(myset):
    myset.add(1)


db = mydb.DB()

db.insert("12",{"324":"624"})
db.insert("7",{"erersfdg":"ttrsdfsdfg"})
db.insert("4",{"1":"sdfgsdfg", "324":"rrrrr", "erersfdg":"tttttt"})

db.insert("5",{"1":"rwetwert"})
db.insert("3",{"1":"eeee"}) 
db.insert("10",{"324":"524"})

db.insert("9",{"1":"fucksdfgsdfg", "324":"jjrrr", "erersfdg":"tttttt"})
db.insert("8",{"324":"424"})
db.insert("7",{"1":"asdfg", "324":"qqqq", "erersfdg":"t55ttt"})

db.insert("23",{"1":"rererrere", "324":"278", "erersfdg":"t88ttt"})
db.insert("35",{"1":"r9r999", "324":"276", "erersfdg":"t99ttt"})
db.insert("66",{"2":"234234", "324":"275", "erersfdg":"t22ttt"})

#immutable
db.insert("23",{"1":"234234", "324":"222", "erersfdg":"522ttt"})
db.insert("11",{"1":"wwweesff", "324":"666", "erersfdg":"699ttt"})
db.insert("12",{"2":"234234", "324":"888", "erersfdg":"722ttt"})

#mutable
db.insert("12",{"1":"www4234", "324":"777", "erersfdg":"922ttt"})

db.remove("7")
print(db.query("7"))
