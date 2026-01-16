"""
데이터베이스 수집 메인 모듈
"""
import os
import sqlite3
import yaml
from pathlib import Path
from typing import Dict, List, Any
import logging
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()


class DBCollector:
    """데이터베이스에서 데이터를 수집하여 SQLite에 저장"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Args:
            config_path: 설정 파일 경로
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.sqlite_conn = None
        
    def _load_config(self, config_path: str) -> Dict:
        """설정 파일 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self):
        """로깅 설정"""
        log_level = self.config.get('options', {}).get('log_level', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_sqlite(self):
        """SQLite 데이터베이스 연결"""
        sqlite_path = self.config['sqlite']['output_path']
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        
        self.sqlite_conn = sqlite3.connect(sqlite_path)
        self.logger.info(f"SQLite 연결 성공: {sqlite_path}")
        return self.sqlite_conn
    
    def get_source_connection(self, db_name: str):
        """소스 데이터베이스 연결"""
        db_config = self.config['databases'][db_name]
        db_type = db_config['type']
        
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
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 타입: {db_type}")
        
        self.logger.info(f"{db_name} 연결 성공")
        return conn
    
    def load_sql_files(self) -> Dict[str, str]:
        """SQL 디렉터리에서 SQL 파일들을 로드"""
        sql_dir = Path(self.config['sql_directory'])
        sql_files = {}
        
        if not sql_dir.exists():
            self.logger.warning(f"SQL 디렉터리가 없습니다: {sql_dir}")
            return sql_files
        
        for sql_file in sql_dir.glob('*.sql'):
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_files[sql_file.stem] = f.read()
            self.logger.info(f"SQL 파일 로드: {sql_file.name}")
        
        return sql_files

    def execute_and_collect(self, db_name: str, sql_name: str, sql_query: str, table_name: str = None):
        """
        소스 DB에서 쿼리 실행 후 결과를 SQLite에 저장
        
        Args:
            db_name: 소스 데이터베이스 이름
            sql_name: SQL 파일 이름
            sql_query: 실행할 SQL 쿼리
            table_name: SQLite에 저장할 테이블 이름 (기본값: sql_name)
        """
        if table_name is None:
            table_name = sql_name
        
        source_conn = None
        try:
            # 소스 DB 연결 및 쿼리 실행
            source_conn = self.get_source_connection(db_name)
            cursor = source_conn.cursor()
            
            self.logger.info(f"쿼리 실행 중: {sql_name}")
            cursor.execute(sql_query)
            
            # 결과 가져오기
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            self.logger.info(f"조회된 행 수: {len(rows)}")
            
            # SQLite에 저장
            self._save_to_sqlite(table_name, columns, rows)
            
            return len(rows)
            
        except Exception as e:
            self.logger.error(f"쿼리 실행 실패 ({sql_name}): {str(e)}")
            raise
        finally:
            if source_conn:
                source_conn.close()
    
    def _save_to_sqlite(self, table_name: str, columns: List[str], rows: List[tuple]):
        """SQLite에 데이터 저장"""
        if not self.sqlite_conn:
            self.connect_sqlite()
        
        cursor = self.sqlite_conn.cursor()
        
        # 테이블 생성 (이미 존재하면 삭제 후 재생성)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # 컬럼 정의 생성
        columns_def = ", ".join([f"{col} TEXT" for col in columns])
        create_table_sql = f"CREATE TABLE {table_name} ({columns_def})"
        cursor.execute(create_table_sql)
        
        # 데이터 삽입
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        
        batch_size = self.config.get('options', {}).get('batch_size', 1000)
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            self.sqlite_conn.commit()
        
        self.logger.info(f"SQLite에 저장 완료: {table_name} ({len(rows)} 행)")
    
    def run_all(self, db_name: str):
        """모든 SQL 파일 실행"""
        sql_files = self.load_sql_files()
        
        if not sql_files:
            self.logger.warning("실행할 SQL 파일이 없습니다")
            return
        
        results = {}
        for sql_name, sql_query in sql_files.items():
            try:
                row_count = self.execute_and_collect(db_name, sql_name, sql_query)
                results[sql_name] = {'status': 'success', 'rows': row_count}
            except Exception as e:
                results[sql_name] = {'status': 'failed', 'error': str(e)}
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.logger.info("SQLite 연결 종료")


if __name__ == "__main__":
    # 사용 예시
    collector = DBCollector()
    
    try:
        # 모든 SQL 파일 실행
        results = collector.run_all('source_db')
        
        # 결과 출력
        print("\n=== 수집 결과 ===")
        for sql_name, result in results.items():
            if result['status'] == 'success':
                print(f"✓ {sql_name}: {result['rows']} 행 수집")
            else:
                print(f"✗ {sql_name}: {result['error']}")
    
    finally:
        collector.close()
