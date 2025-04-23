"""
asyncpg 기반 비동기 데이터베이스 커넥션 풀 관리 모듈

이 모듈은 PostgreSQL 데이터베이스에 대한 비동기 연결을 asyncpg를 통해 제공합니다.
싱글턴 패턴을 사용하여 애플리케이션 전체에서 하나의 커넥션 풀만 생성되도록 합니다.
"""

import os
from typing import Dict, Optional, Any, AsyncGenerator
from dotenv import load_dotenv

import asyncpg
from asyncpg.pool import Pool

# 환경 변수 로드
load_dotenv()

# 글로벌 변수로 커넥션 풀 선언
_pool: Optional[Pool] = None


def _get_connection_params() -> Dict[str, Any]:
    """
    환경 변수에서 DB 연결 정보를 가져와 asyncpg 연결 매개변수 딕셔너리를 생성합니다.
    
    Returns:
        Dict[str, Any]: asyncpg 연결 매개변수 딕셔너리
        
    Raises:
        ValueError: 필수 환경 변수가 설정되지 않은 경우
    """
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    
    # 필수 환경 변수 검증
    missing_vars = []
    if not db_host: missing_vars.append("DB_HOST")
    if not db_port: missing_vars.append("DB_PORT")
    if not db_user: missing_vars.append("DB_USER")
    if not db_password: missing_vars.append("DB_PASSWORD")
    if not db_name: missing_vars.append("DB_NAME")
    
    if missing_vars:
        raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
    
    # asyncpg 연결 매개변수 생성
    return {
        "host": db_host,
        "port": int(db_port),
        "user": db_user,
        "password": db_password,
        "database": db_name,
    }


async def get_pool() -> Pool:
    """
    asyncpg 커넥션 풀의 싱글턴 인스턴스를 반환합니다.
    
    Returns:
        Pool: asyncpg 비동기 커넥션 풀 인스턴스
        
    Raises:
        ValueError: 필수 환경 변수가 설정되지 않은 경우
    """
    global _pool
    
    if _pool is None:
        # 환경 변수로부터 연결 매개변수 생성
        connection_params = _get_connection_params()
        
        # 커넥션 풀 생성
        _pool = await asyncpg.create_pool(
            **connection_params,
            min_size=5,       # 최소 커넥션 수
            max_size=20,      # 최대 커넥션 수
            timeout=30.0,     # 커넥션 타임아웃 (초)
            command_timeout=60.0,  # 쿼리 실행 타임아웃 (초)
            max_inactive_connection_lifetime=1800.0,  # 비활성 커넥션 만료 시간 (초)
        )
        print("[INFO] asyncpg 커넥션 풀이 초기화되었습니다.")
    
    return _pool


async def get_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    요청 처리를 위한 비동기 데이터베이스 커넥션을 제공합니다.
    
    Yields:
        asyncpg.Connection: 비동기 asyncpg 커넥션
        
    Examples:
        ```python
        async with get_connection() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
        ```
        
        또는 FastAPI 의존성 주입 시:
        
        ```python
        @app.get("/users/{user_id}")
        async def get_user(user_id: int, conn: asyncpg.Connection = Depends(get_connection)):
            user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            return dict(user)
        ```
    """
    pool = await get_pool()
    async with pool.acquire() as connection:
        try:
            yield connection
        finally:
            # 커넥션은 with 블록 종료 시 자동으로 풀로 반환됨
            pass 