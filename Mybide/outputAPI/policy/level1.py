# import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import re
import pandas as pd


def main1(search, user_id):

    @F.udf('string')
    def change_name(x):
        return x[:1]+'씨' #한씨

    @F.udf('string')
    def change_age(x):
        return str(2021-int(x[:4])+1)+'살'  #25살

    @F.udf('string')
    def change_addr(x):
        x2=x.split(' ')[:3]
        return ' '.join(x2)  #충북 청주시 사운로359번길

    @F.udf()
    def image_tag(x,y,a,b,c,search):
        # 검색어가 개인정보에 있을 때
        if (search in y) or (search in a) or (search in b) or (search in c) :
            path_list = []
            p = re.compile('({)(.*?)(})')
            m = p.findall(x)
            for i in range(len(m)):
                image_info = m[i][1]
                # print(m[i][1])
                # 검색어가 개인정보에도 있고 이미지 태그에도 있을 때
                if search in image_info:
                    temp = image_info.split(',')[0]
                    image_path = temp.split(':')[1]
                    #print(image_path)
                    path_list.append(image_path)
                temp = image_info.split(',')[0]
                image_path = temp.split(':')[1]
                #print(image_path)
                path_list.append(image_path)
            return set(path_list) #중복 이미지경로 제거
        else:
            # 검색어가 이미지 태그에만 있을 때
            path_list = []
            p = re.compile('({)(.*?)(})')
            m = p.findall(x)
            for i in range(len(m)):
                image_info = m[i][1]
                if search in image_info:
                    temp = image_info.split(',')[0]
                    image_path = temp.split(':')[1]
                    # print(image_path)
                    path_list.append(image_path)
                else:
                    pass
            if len(path_list) <1:
                #검색어에 해당하는 이미지 없을 때, null값 넣기 
                return None
            else:
                return set(path_list)

    print('start search')
    print('start loading CSV File')

    df = spark.read.csv('policy/plat_id.csv',header=True)
    df1 = spark.read.csv('policy/image_data.csv',header=True)

    df = df.withColumn('name',change_name(F.col('name')))
    df = df.withColumn('age',change_age(F.col('age')))
    df = df.withColumn('address',change_addr(F.col('address')))   
    df = df.join(df1,on=['id'],how='inner')
    
    # 태그 검색
    df = df.withColumn('image',image_tag(F.col('image'),F.col('name'),F.col('age'),F.col('sex'),F.col('address'),F.lit(search)))
    

    df = df.drop('pwd','created_at','id','_id')  #해당 컬럼 제거
    # df = df.filter(F.col('name').isNotNull())
    df = df.filter(F.col('image').isNotNull())
    
    # df.show(20,False)
    # df.show()
    
    # df.coalesce(1).write.mode('overwrite').option("header", "true").csv(user_id+"_deid.csv")
    result_pdf = df.select("*").toPandas()
    
    image_list = []
    for string_path in result_pdf['image'] :
    # string_path = result_pdf['image'][0]
        p = re.compile("(')(.*?)(')")
        m = p.findall(string_path)
        for i in range(len(m)):
            image_list.append(m[i][1])
            # print(m[i][1])

    return image_list # ['test1.png', 'test11.png']

# tag = input("tag -> ")
# main1('서울',"")
