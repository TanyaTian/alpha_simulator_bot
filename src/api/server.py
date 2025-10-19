from flask import Flask

# 导入蓝图
from api.config_api import config_bp
from api.alpha_api import alpha_api_blueprint as alpha_bp

# 创建 Flask 应用
app = Flask(__name__)

# 注册蓝图
app.register_blueprint(config_bp)
app.register_blueprint(alpha_bp)

def run_server(port=5001):
    """启动 Flask API 服务"""
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    run_server()