"""
데이터베이스 접속 정보 관리 모듈
"""
import yaml
import os
from pathlib import Path
from typing import Dict, List


class DBConfigManager:
    """데이터베이스 설정 관리 클래스"""
    
    def __init__(self, config_path: str = "conf/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        if not os.path.exists(self.config_path):
            return {'databases': {}, 'sqlite': {'output_path': '../data/collected_data.db'}, 
                    'sql_directory': 'sql_queries', 'options': {}}
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _save_config(self):
        """설정 파일 저장"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
    
    def list_databases(self) -> List[str]:
        """등록된 데이터베이스 목록 조회"""
        return list(self.config.get('databases', {}).keys())
    
    def get_database(self, db_name: str) -> Dict:
        """특정 데이터베이스 정보 조회"""
        return self.config.get('databases', {}).get(db_name)
    
    def add_database(self, db_name: str, db_config: Dict):
        """데이터베이스 추가"""
        if 'databases' not in self.config:
            self.config['databases'] = {}
        
        self.config['databases'][db_name] = db_config
        self._save_config()
        print(f"✓ 데이터베이스 '{db_name}' 추가 완료")
    
    def update_database(self, db_name: str, db_config: Dict):
        """데이터베이스 정보 수정"""
        if db_name not in self.config.get('databases', {}):
            raise ValueError(f"데이터베이스 '{db_name}'이 존재하지 않습니다")
        
        self.config['databases'][db_name] = db_config
        self._save_config()
        print(f"✓ 데이터베이스 '{db_name}' 수정 완료")
    
    def delete_database(self, db_name: str):
        """데이터베이스 삭제"""
        if db_name not in self.config.get('databases', {}):
            raise ValueError(f"데이터베이스 '{db_name}'이 존재하지 않습니다")
        
        del self.config['databases'][db_name]
        self._save_config()
        print(f"✓ 데이터베이스 '{db_name}' 삭제 완료")
    
    def test_connection(self, db_name: str) -> bool:
        """데이터베이스 연결 테스트"""
        db_config = self.get_database(db_name)
        if not db_config:
            print(f"✗ 데이터베이스 '{db_name}'이 존재하지 않습니다")
            return False
        
        db_type = db_config['type']
        
        try:
            if db_type == 'postgresql':
                import psycopg2
                conn = psycopg2.connect(
                    host=db_config['host'],
                    port=db_config['port'],
                    database=db_config['database'],
                    user=db_config['user'],
                    password=db_config['password']
                )
            elif db_type == 'mysql':
                import pymysql
                conn = pymysql.connect(
                    host=db_config['host'],
                    port=db_config['port'],
                    database=db_config['database'],
                    user=db_config['user'],
                    password=db_config['password']
                )
            elif db_type == 'oracle':
                import cx_Oracle
                if 'service_name' in db_config:
                    dsn = cx_Oracle.makedsn(
                        db_config['host'],
                        db_config['port'],
                        service_name=db_config['service_name']
                    )
                else:
                    dsn = cx_Oracle.makedsn(
                        db_config['host'],
                        db_config['port'],
                        sid=db_config['sid']
                    )
                conn = cx_Oracle.connect(
                    user=db_config['user'],
                    password=db_config['password'],
                    dsn=dsn
                )
            elif db_type == 'mssql':
                import pyodbc
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={db_config['host']},{db_config['port']};"
                    f"DATABASE={db_config['database']};"
                    f"UID={db_config['user']};"
                    f"PWD={db_config['password']}"
                )
                conn = pyodbc.connect(conn_str)
            else:
                print(f"✗ 지원하지 않는 데이터베이스 타입: {db_type}")
                return False
            
            conn.close()
            print(f"✓ 데이터베이스 '{db_name}' 연결 성공")
            return True
            
        except Exception as e:
            print(f"✗ 데이터베이스 '{db_name}' 연결 실패: {str(e)}")
            return False


def interactive_add_database():
    """대화형 데이터베이스 추가"""
    manager = DBConfigManager()
    
    print("\n=== 데이터베이스 접속 정보 등록 ===\n")
    
    db_name = input("데이터베이스 이름: ").strip()
    if not db_name:
        print("✗ 데이터베이스 이름은 필수입니다")
        return
    
    print("\n지원하는 데이터베이스 타입:")
    print("1. PostgreSQL")
    print("2. MySQL")
    print("3. Oracle")
    print("4. SQL Server")
    
    db_type_choice = input("\n선택 (1-4): ").strip()
    db_type_map = {
        '1': 'postgresql',
        '2': 'mysql',
        '3': 'oracle',
        '4': 'mssql'
    }
    
    db_type = db_type_map.get(db_type_choice)
    if not db_type:
        print("✗ 잘못된 선택입니다")
        return
    
    host = input("호스트 (기본값: localhost): ").strip() or "localhost"
    
    # 기본 포트 설정
    default_ports = {
        'postgresql': 5432,
        'mysql': 3306,
        'oracle': 1521,
        'mssql': 1433
    }
    default_port = default_ports[db_type]
    port_input = input(f"포트 (기본값: {default_port}): ").strip()
    port = int(port_input) if port_input else default_port
    
    database = input("데이터베이스명: ").strip()
    user = input("사용자명: ").strip()
    password = input("비밀번호: ").strip()
    
    db_config = {
        'type': db_type,
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    
    # Oracle 추가 설정
    if db_type == 'oracle':
        service_type = input("연결 방식 (1: service_name, 2: sid) [기본값: 1]: ").strip() or '1'
        if service_type == '1':
            service_name = input("Service Name: ").strip()
            db_config['service_name'] = service_name
        else:
            sid = input("SID: ").strip()
            db_config['sid'] = sid
    
    # SQL Server 추가 설정
    if db_type == 'mssql':
        windows_auth = input("Windows 인증 사용? (y/n) [기본값: n]: ").strip().lower()
        if windows_auth == 'y':
            db_config['trusted_connection'] = True
    
    # 설정 저장
    manager.add_database(db_name, db_config)
    
    # 연결 테스트
    test = input("\n연결 테스트를 진행하시겠습니까? (y/n) [기본값: y]: ").strip().lower()
    if test != 'n':
        manager.test_connection(db_name)


def main_menu():
    """메인 메뉴"""
    manager = DBConfigManager()
    
    while True:
        print("\n" + "="*50)
        print("데이터베이스 접속 정보 관리")
        print("="*50)
        print("1. 데이터베이스 목록 조회")
        print("2. 데이터베이스 추가")
        print("3. 데이터베이스 정보 조회")
        print("4. 데이터베이스 삭제")
        print("5. 연결 테스트")
        print("0. 종료")
        print("="*50)
        
        choice = input("\n선택: ").strip()
        
        if choice == '1':
            databases = manager.list_databases()
            if databases:
                print("\n등록된 데이터베이스:")
                for i, db_name in enumerate(databases, 1):
                    db_info = manager.get_database(db_name)
                    print(f"{i}. {db_name} ({db_info['type']} - {db_info['host']}:{db_info['port']})")
            else:
                print("\n등록된 데이터베이스가 없습니다")
        
        elif choice == '2':
            interactive_add_database()
        
        elif choice == '3':
            db_name = input("조회할 데이터베이스 이름: ").strip()
            db_info = manager.get_database(db_name)
            if db_info:
                print(f"\n=== {db_name} 정보 ===")
                for key, value in db_info.items():
                    if key == 'password':
                        print(f"{key}: {'*' * len(str(value))}")
                    else:
                        print(f"{key}: {value}")
            else:
                print(f"✗ 데이터베이스 '{db_name}'이 존재하지 않습니다")
        
        elif choice == '4':
            db_name = input("삭제할 데이터베이스 이름: ").strip()
            confirm = input(f"정말로 '{db_name}'을(를) 삭제하시겠습니까? (y/n): ").strip().lower()
            if confirm == 'y':
                try:
                    manager.delete_database(db_name)
                except ValueError as e:
                    print(f"✗ {str(e)}")
        
        elif choice == '5':
            db_name = input("테스트할 데이터베이스 이름: ").strip()
            manager.test_connection(db_name)
        
        elif choice == '0':
            print("\n프로그램을 종료합니다")
            break
        
        else:
            print("✗ 잘못된 선택입니다")


if __name__ == "__main__":
    main_menu()
