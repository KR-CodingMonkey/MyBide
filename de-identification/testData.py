from pymongo import MongoClient   
from datetime import datetime      
import csv 

client = MongoClient('localhost', 27017)  
db = client.db_test                      # 'dbsparta'라는 이름의 db를 만듭니다.

now = datetime.now()
nowDate = now.strftime('%Y-%m-%d')

# MongoDB에 insert 하기
# 'hair'라는 collection에 데이터를 넣는다.
#########################################################################
db.hair.insert_one({'name':'한지혜','age':'19970816','sex':'F','address':'충북 청주시 사운로359번길 13 107동 1406호','phone_num':'01049389226','created_at':nowDate,'image':'test1.img'})
db.hair.insert_one({'name':'이상협','age':'19960917','sex':'M','address':'경기 용인시 동백중앙로 41 210동 203호','phone_num':'01012341234','created_at':nowDate,'image':'test2.img'})
db.hair.insert_one({'name':'김재원','age':'19951123','sex':'M','address':'서울시 관악구 호암로 399 30동 1103호','phone_num':'01045465555','created_at':nowDate,'image':'test3.img'})
db.hair.insert_one({'name':'한종민','age':'19920411','sex':'M','address':'서울 송파구 송파대로 567 209동 408호','phone_num':'01098985656','created_at':nowDate,'image':'test4.img'})
#########################################################################

#db.hair.update_one({'name':'한지혜'},{'$set':{'address':'충북 청주시 사운로359번길 13 107동 1406호' }})
#db.hair.update_one({'name':'이상협'},{'$set':{'address':'경기 용인시 동백중앙로 41 210동 203호' }})
#db.hair.update_one({'name':'김재원'},{'$set':{'address':'서울시 관악구 호암로 399 30동 1103호' }})
#db.hair.update_one({'name':'한종민'},{'$set':{'address':'서울 송파구 송파대로 567 209동 408호' }})

list_field = []
doc=db.hair.find_one()
for i in doc:
    list_field.append(i)

cursor = db.hair.find ({})
cursor = list(cursor)

with open('mongodb_hair.csv', 'w', encoding='utf-8-sig') as outfile:   

    fields = list_field
    write = csv.DictWriter(outfile, fieldnames=fields)
    write.writeheader()
    for x in cursor: 
        print(x)
        write.writerow(x)