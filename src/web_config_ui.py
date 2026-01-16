"""
웹 기반 데이터베이스 접속 정보 관리 UI
"""
from flask import Flask, render_template, request, jsonify, redirect, url_for
from db_config_manager import DBConfigManager
import os

app = Flask(__name__, template_folder='../templates')
manager = DBConfigManager()


@app.route('/')
def index():
    """메인 페이지"""
    databases = manager.list_databases()
    db_list = []
    for db_name in databases:
        db_info = manager.get_database(db_name)
        db_list.append({
            'name': db_name,
            'type': db_info['type'],
            'host': db_info['host'],
            'port': db_info['port'],
            'database': db_info['database']
        })
    return render_template('index.html', databases=db_list)


@app.route('/add', methods=['GET', 'POST'])
def add_database():
    """데이터베이스 추가"""
    if request.method == 'POST':
        data = request.form
        db_name = data.get('db_name')
        db_type = data.get('db_type')
        
        db_config = {
            'type': db_type,
            'host': data.get('host'),
            'port': int(data.get('port')),
            'database': data.get('database'),
            'user': data.get('user'),
            'password': data.get('password')
        }
        
        # Oracle 추가 설정
        if db_type == 'oracle':
            if data.get('service_name'):
                db_config['service_name'] = data.get('service_name')
            elif data.get('sid'):
                db_config['sid'] = data.get('sid')
        
        # SQL Server 추가 설정
        if db_type == 'mssql' and data.get('trusted_connection'):
            db_config['trusted_connection'] = True
        
        manager.add_database(db_name, db_config)
        return redirect(url_for('index'))
    
    return render_template('add_database.html')


@app.route('/edit/<db_name>', methods=['GET', 'POST'])
def edit_database(db_name):
    """데이터베이스 수정"""
    if request.method == 'POST':
        data = request.form
        db_type = data.get('db_type')
        
        db_config = {
            'type': db_type,
            'host': data.get('host'),
            'port': int(data.get('port')),
            'database': data.get('database'),
            'user': data.get('user'),
            'password': data.get('password')
        }
        
        if db_type == 'oracle':
            if data.get('service_name'):
                db_config['service_name'] = data.get('service_name')
            elif data.get('sid'):
                db_config['sid'] = data.get('sid')
        
        if db_type == 'mssql' and data.get('trusted_connection'):
            db_config['trusted_connection'] = True
        
        manager.update_database(db_name, db_config)
        return redirect(url_for('index'))
    
    db_info = manager.get_database(db_name)
    return render_template('edit_database.html', db_name=db_name, db_info=db_info)


@app.route('/delete/<db_name>', methods=['POST'])
def delete_database(db_name):
    """데이터베이스 삭제"""
    try:
        manager.delete_database(db_name)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/test/<db_name>', methods=['POST'])
def test_connection(db_name):
    """연결 테스트"""
    success = manager.test_connection(db_name)
    return jsonify({'success': success})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
