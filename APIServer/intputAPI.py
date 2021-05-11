from flask  import Flask, render_template, jsonify, request
from pymongo import MongoClient 
from flask_cors import CORS, cross_origin

import os
import uuid
import datetime

client = MongoClient('localhost', 27017) # windows
# client = MongoClient('localhost', 17017) # ubuntu
db = client.user_login_system # DB 연결

app = Flask(__name__)
CORS(app)

current_dir = os.getcwd() + '/image/'
now = datetime.datetime.now()
nowDate = now.strftime('%Y-%m-%d')

def Sign_Up():
    # print(request.form)

    # 사용자 객체 생성 fix-it (변수로 대체, 기본값 설정)
    user = {
        "_id" : uuid.uuid4().hex,
        "name" : request.form.get('name'),
        "email" : request.form.get('email'),        
        "sex" : request.form.get('sex'),
        "age" : request.form.get('year_of_birth'),
        "address" : request.form.get('address'),
        "phone_num" : request.form.get('phone_num'),
        "created_at": nowDate
    }

    check_email = db.users.find_one({"email": user["email"]}) # 기존에 있는 데이터인지 확인 여부

    if check_email:
        print('123')
        return jsonify(user), 300
    else:
        db.users.insert_one(user)
        return jsonify(user), 200

# @app.route('/')
# def hello_world():
#     return render_template('custom.html')

# 고객 등록
@app.route("/user/signup", methods=['POST'])
def signup():
  return Sign_Up() # fix-it

# @app.route('/upload', methods=['GET'])
# def upload():
#     return render_template('upload.html')

# fix-it  
@app.route('/upload/complete', methods=['POST'])
def upload_complete():

    # print(request.form.getlist("key"))
    # print(request.form.getlist("value"))

    # 이미지 받기 request.files[name]
    pic = request.files['pic']
    keys = request.form.getlist("key")
    values = request.form.getlist("value")

    # 이메일 검색 -> DB 찾아서 -> 파일저장
    custom_email = request.form.get("Email")
    confirm_email = db.users.find_one({"email": custom_email})

    if custom_email in confirm_email['email']:

        directory_list = os.listdir(current_dir) # 현재 경로에 모든폴더 리스트
        image_dir = current_dir + custom_email +'/' # image\test@naver.com
        
        # 신규 고객일 경우 (업로드가 처음)
        if custom_email not in directory_list:
            os.mkdir(image_dir)
            pic.save(image_dir + '1.png')

        else:
            count_img = len(os.listdir(image_dir))
            new_filename = str(count_img + 1) + '.png'
            pic.save(image_dir + new_filename)
            
    metatag = {'image_path' : new_filename} # 이미지 경로, 초기화
    for key, value in zip(keys, values):
        metatag[key] = value
 
    db.users.update_one({"email":custom_email}, {'$push':{"image":metatag}}) # update_one(조건문, 수정내용)
    return jsonify(metatag), 200

# @app.route("/test")
# def test():
#     return "This is test page."

if __name__ == "__main__":
    app.run(host = '0.0.0.0', debug=True, port= 5000)


