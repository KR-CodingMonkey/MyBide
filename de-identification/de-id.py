import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import re
import math
import pandas as pd
import random
from datetime import datetime, timedelta

#--------------name 비식별 기법--------------#
# masking_level1_name
@F.udf('string')
def masking_level1_name(x):
    return x[:1]+'XX'

# masking_level2_name
@F.udf('string')
def masking_level2_name(x):
    return 'XXX'

#--------------age 비식별 기법---------------#
# 출생년도만 출력
@F.udf('string')
def change_birth(x):
    return (x[:4])

# rounding_level0_age 
@F.udf('int')
def rounding_level0_age(x):
    return 2021-int(x[:4])

#rounding_level1_age
rounding_level1_age = F.udf(lambda x, step:round(int(x),step))

#--------------sex 비식별 기법---------------#
#sex-level1
@F.udf('int')
def code_level1_sex(x):
    if x is None: return 20
    return random.randrange(10) + (10 if x=='M' else 0) 
    # 여자는 0~9, 남자는 10~19, 없으면 20    


#------------address 비식별 기법-------------#
#del_level1_addr
@F.udf('string')
def del_level1_addr(x):
    x2=x.split(' ')[:3]
    return ' '.join(x2)   

#del_level2_addr
@F.udf('string')
def del_level2_addr(x):
    x2=x.split(' ')[:2]
    return ' '.join(x2)  

#del_level3_addr
@F.udf('string')
def del_level3_addr(x):
    x2=x.split(' ')[:1]
    return ' '.join(x2)  

#-----------phone_num 비식별 기법------------#
# masking_level1_phn
@F.udf('string')
def masking_level1_phn(x):
    return x[:7]+'XXXX' 

# masking_level2_phn
@F.udf('string')
def masking_level2_phn(x):
    return x[:3]+'XXXXXXXX' 

#scrumb_level1_phn
@F.udf('string')
def scrumb_level1_phn(x):
    x2 = list(x[7:])
    random.shuffle(x2)
    return x[:7]+''.join(x2)

#scrumb_level2_phn
@F.udf('string')
def scrumb_level2_phn(x):
    x2 = list(x[3:])
    random.shuffle(x2)
    return x[:3]+''.join(x2)

#-----------created_at 비식별 기법------------#

# noise_level1_date
@F.udf('date')
def noise_level1_date(x):
    date = datetime.strptime(x,'%Y-%m-%d')
    return date + timedelta(days=random.randrange(-3,3))

# noise_level2_date
@F.udf('date')
def noise_level2_date(x):
    date = datetime.strptime(x,'%Y-%m-%d')
    return date + timedelta(days=random.randrange(-7,7))

# del_level1_date
@F.udf('string')
def del_level1_date(x):
    return x[:7]

# del_level2_date
@F.udf('string')
def del_level2_date(x):
    return x[:4]
#---------------------------------------------#


file_name = input("비식별할 파일 이름 입력 : ")
df = spark.read.csv(file_name+".csv",header=True)
df2 = spark.read.csv(file_name+".csv",header=True) # level별 데이터 출력


global select_list # 비식별화 대상 컬럼
select_list=[]

print("\n-> 데이터 확인")
df.drop('_id').show()
field_num = input("\n비식별 대상 필드 갯수 입력 : ")
print("\n")
field_num=int(field_num)

for num in range(field_num):
    field_name = input("비식별 대상 필드 이름 입력(하나씩) :")
    select_list.append(field_name)
# print(select_list)

############################ 'name' 컬럼 비식별화 ############################
if 'name' in select_list:
    name_check1 = int(input("\n---------------------\n컬럼이름: name\n---------------------\n데이터 타입을 선택하세요.\n 1) 식별자\n-> "))
    if name_check1 ==1: # 식별자 선택
        name_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 마스킹\n 2) 컬럼삭제\n-> "))
        if name_check2 == 1: # 마스킹 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2")
            # 비식별 조치 수준별 데이터 확인
            df2= df.withColumn('Level-0', F.col('name'))
            df2 = df2.withColumn('Level-1',masking_level1_name(F.col('name')))
            df2 = df2.withColumn('Level-2',masking_level2_name(F.col('name')))
            df2.select('Level-0','Level-1','Level-2').show()
            name_check3 = int(input("-> "))
            if name_check3 == 1: # 마스킹 -> level-0
                pass
            if name_check3 == 2: # 마스킹 -> level-1
                df = df.withColumn('name',masking_level1_name(F.col('name')))
            if name_check3 ==3: # 마스킹 -> level-2
                df = df.withColumn('name',masking_level2_name(F.col('name')))
        if name_check2 == 2: # 컬럼삭제 선택
             select_list.remove('name')           


############################ 'age' 컬럼 비식별화 #############################
if 'age' in select_list:
    df = df.withColumn('birth_year',change_birth(F.col('age'))) # 생년월일에서 출생년도만 get
    age_check1 = int(input("\n---------------------\n컬럼이름: age\n---------------------\n데이터 타입을 선택하세요.\n 1) 준식별자\n-> "))
    if age_check1 ==1: # 준식별자 선택
        age_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 라운딩\n 2) 컬럼삭제\n-> "))
        if age_check2 == 1: # 라운딩 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',rounding_level0_age(F.col('age')))
            df2 = df2.withColumn('age',rounding_level0_age(F.col('age'))) # 출생년도를 나이로 바꾸고
            df2 = df2.withColumn('Level-1',rounding_level1_age(F.col('age'),F.lit(-1)))
            df2.select('Level-0','Level-1').show()
            age_check3 = int(input("-> "))
            if age_check3 == 1: # 라운딩 -> level-0 선택
                df = df.withColumn('age',rounding_level0_age(F.col('age')))
            if age_check3 == 2: # 라운딩 -> level-1 선택
                df = df.withColumn('age',rounding_level0_age(F.col('age')))
                df = df.withColumn('age',rounding_level1_age(F.col('age'),F.lit(-1))) 
                # 10단위로 라운딩(1의 자리에서 반올림)
        if age_check2 == 2: # 컬럼삭제 선택했을 시.
            select_list.remove('age')      


########################## 'address' 컬럼 비식별화 ###########################
if 'address' in select_list:
    addr_check1 = int(input("\n---------------------\n컬럼이름: address\n---------------------\n데이터 타입을 선택하세요.\n 1) 식별자\n 2) 준식별자\n-> "))
    if addr_check1 ==1: # 식별자 선택
        addr_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 부분삭제\n 2) 컬럼삭제\n-> "))
        if addr_check2 == 1: # 부분삭제 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2\n 4) Level-3")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('address'))
            df2 = df2.withColumn('Level-1',del_level1_addr(F.col('address'))) 
            df2 = df2.withColumn('Level-2',del_level2_addr(F.col('address')))
            df2 = df2.withColumn('Level-3',del_level3_addr(F.col('address')))
            df2.select('Level-0','Level-1','Level-2','Level-3').show()
            addr_check3 = int(input("-> "))
            if addr_check3 == 1: # 부분삭제 -> level-0 선택
                pass
            if addr_check3 == 2: # 부분삭제 -> level-1 선택
                df = df.withColumn('address',del_level1_addr(F.col('address')))
            if addr_check3 == 3: # 부분삭제 -> level-2 선택
                df = df.withColumn('address',del_level2_addr(F.col('address')))
            if addr_check3 == 4: # 부분삭제 -> level-3 선택
                df = df.withColumn('address',del_level3_addr(F.col('address')))       
        if addr_check2 == 2: # 컬럼삭제 선택했을 시.
            select_list.remove('address')   
    if addr_check1 ==2: # 준식별자 선택
        addr_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 부분삭제\n 2) 컬럼삭제\n-> "))
        if addr_check2 == 1: # 부분삭제 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',del_level2_addr(F.col('address')))
            df2 = df2.withColumn('Level-1',del_level3_addr(F.col('address')))
            df2.select('Level-0','Level-1').show()
            addr_check3 = int(input("-> "))
            if addr_check3 == 1: # 부분삭제 -> level-0 선택
                df = df.withColumn('address',del_level2_addr(F.col('address')))
            if addr_check3 == 2: # 부분삭제 -> level-1 선택
                df = df.withColumn('address',del_level3_addr(F.col('address')))
        if addr_check2 ==2: # 컬럼삭제 선택
            select_list.remove('address')


############################ 'sex' 컬럼 비식별화 #############################
if 'sex' in select_list:
    sex_check1 = int(input("\n---------------------\n컬럼이름: sex\n---------------------\n데이터 타입을 선택하세요.\n 1) 준식별자\n-> "))
    if sex_check1 ==1: # 준식별자 선택
        sex_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 코드화\n 2) 컬럼삭제\n-> "))
        if sex_check2 == 1: # 코드화 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('sex'))
            df2 = df2.withColumn('Level-1',code_level1_sex(F.col('sex')))
            df2.select('Level-0','Level-1').show()
            sex_check3 = int(input("-> "))
            if sex_check3 == 1: # 코드화 -> level-0 선택
                pass
            if sex_check3 == 2: # 코드화 -> level-1 선택
                df = df.withColumn('sex',code_level1_sex(F.col('sex')))
        if sex_check2 == 2: # 컬럼삭제 선택했을 시.
            select_list.remove('sex')      


######################### 'phone_num' 컬럼 비식별화 ##########################
if 'phone_num' in select_list:
    phn_check1 = int(input("\n---------------------\n컬럼이름: phone_num\n---------------------\n데이터 타입을 선택하세요.\n 1) 식별자\n-> "))
    if phn_check1 ==1: # 식별자 선택
        phn_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 마스킹\n 2) 스크램블링\n 3) 컬럼삭제\n-> "))
        if phn_check2 == 1: # 마스킹 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('phone_num'))
            df2 = df2.withColumn('Level-1',masking_level1_phn(F.col('phone_num')))
            df2 = df2.withColumn('Level-2',masking_level2_phn(F.col('phone_num')))
            df2.select('Level-0','Level-1','Level-2').show()
            phn_check3 = int(input("-> "))
            if phn_check3 == 1: # 마스킹 -> level-0 선택
                pass
            if phn_check3 == 2: # 마스킹 -> level-1 선택
                df = df.withColumn('phone_num',masking_level1_phn(F.col('phone_num')))
            if phn_check3 == 3: # 마스킹 -> level-2 선택
                df = df.withColumn('phone_num',masking_level2_phn(F.col('phone_num')))
                pass
        if phn_check2 == 2: # 스크램블링 선택했을 시.
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('phone_num'))
            df2 = df2.withColumn('Level-1',scrumb_level1_phn(F.col('phone_num')))
            df2 = df2.withColumn('Level-2',scrumb_level2_phn(F.col('phone_num')))
            df2.select('Level-0','Level-1','Level-2').show()
            phn_check3=int(input("-> "))
            if phn_check3 ==1: # 스크램블링 -> level-0 선택
                pass
            if phn_check3 ==2: # 스크램블링 -> level-1 선택
                df = df.withColumn('phone_num',scrumb_level1_phn(F.col('phone_num')))
            if phn_check3 ==3: # 스크램블링 -> level-2 선택
                df = df.withColumn('phone_num',scrumb_level2_phn(F.col('phone_num')))
        if phn_check2 == 3: # 컬럼삭제 선택했을 시
            select_list.remove('phone_num')


######################### 'created_at' 컬럼 비식별화 #########################
if 'created_at' in select_list:
    date_check1 = int(input("\n---------------------\n컬럼이름: created_at\n---------------------\n데이터 타입을 선택하세요.\n 1) 준식별자\n-> "))
    if date_check1 ==1: # 준식별자 선택
        date_check2 = int(input("\n비식별화 기술을 선택하세요.\n 1) 잡음추가\n 2) 부분삭제\n 3) 컬럼삭제\n-> "))
        if date_check2 == 1: # 잡음추가 선택
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('created_at'))
            df2 = df2.withColumn('Level-1',noise_level1_date(F.col('created_at')))
            df2 = df2.withColumn('Level-2',noise_level2_date(F.col('created_at')))
            df2.select('Level-0','Level-1','Level-2').show()
            date_check3 = int(input("-> "))
            if date_check3 == 1: # 잡음추가 -> level-0 선택
                pass
            if date_check3 == 2: # 잡음추가 -> level-1 선택
                df = df.withColumn('created_at',noise_level1_date(F.col('created_at')))
            if date_check3 == 3: # 잡음추가 -> level-2 선택
                df = df.withColumn('created_at',noise_level2_date(F.col('created_at')))
        if date_check2 == 2: # 부분삭제 선택했을 시.
            print("\n비식별 조치 수준을 선택하세요.\n 1) Level-0\n 2) Level-1\n 3) Level-2")
            # 비식별 조치 수준별 데이터 확인
            df2 = df2.withColumn('Level-0',F.col('created_at'))
            df2 = df2.withColumn('Level-1',del_level1_date(F.col('created_at')))
            df2 = df2.withColumn('Level-2',del_level2_date(F.col('created_at')))
            df2.select('Level-0','Level-1','Level-2').show()
            date_check3=int(input("-> "))
            if date_check3 ==1: # 부분삭제 -> level-0 선택
                pass
            if date_check3 ==2: # 부분삭제 -> level-1 선택
                df = df.withColumn('created_at',del_level1_date(F.col('created_at')))
            if date_check3 ==3: # 부분삭제 -> level-2 선택
                df = df.withColumn('created_at',del_level2_date(F.col('created_at')))
        if date_check2 == 3: # 컬럼삭제 선택했을 시
            select_list.remove('created_at')

########################################################################


# df.select(select_list).show()
print("\n\n<비식별화 결과>")
df = df.select(select_list)
df.show()
df.coalesce(1).write.mode('overwrite').option("header", "true").csv(file_name+"_deid.csv")

