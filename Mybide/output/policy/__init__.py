from flask import Flask
from flask_migrate import Migrate # 테이블을 생성하고 컬럼을 추가하는 등의 작업
from flask_sqlalchemy import SQLAlchemy # 파이썬 ORM 라이브러리 중 가장 많이 사용

import config

db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__)

    app.config.from_object(config) # config.py 파일에 작성한 항목을 app.config 환경 변수로 부름

    # ORM 초기화
    db.init_app(app)
    migrate.init_app(app, db)

    from . import models

    # 블루프린트
    from .views import main_views, auth_views,img_gal
    app.register_blueprint(main_views.bp)
    app.register_blueprint(auth_views.bp)
    app.register_blueprint(img_gal.bp)

    return app