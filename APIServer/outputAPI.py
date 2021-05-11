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
app.config['JSON_AS_ASCII'] = False # 한글지원
CORS(app)

image_path = os.getcwd() + '/image/'

# load Image
@app.route('/user/loadimage', methods=['GET'])
def Load_Image():
    print('start loading image')
    # customer_email = request.form.get("Email") # 고객 아이디
    customer_email = "ahippo@naver.com" # fix-it; for testing
    confirm_email = db.users.find_one({"email": customer_email}) # 고객 아이디 조회

    if confirm_email:
        image_path2 = image_path + customer_email + '/' #./image/test@naver.com/
        image_path_list = os.listdir(image_path2) # 현재 경로에 모든 이미지 파일 이름
        image_path_list2 = list(map(lambda x: image_path2 + x, image_path_list)) # 절대경로로 만들어주기
        print(image_path_list2)
        return jsonify(image_path_list2)
    else:
        return 'empty Data'

@app.route('/')
def hello_world():
    return 'main page'

if __name__ == "__main__":
    app.run(host = '0.0.0.0', debug=True, port= 5001)


