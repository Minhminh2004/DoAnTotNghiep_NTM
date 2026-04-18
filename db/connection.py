from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError


def create_db_engine(db_url: str):
    return create_engine(db_url, future=True)


def test_connection(db_url: str):
    try:
        engine = create_db_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Kết nối thành công."
    except SQLAlchemyError as e:
        return False, f"Kết nối thất bại: {str(e)}"


def get_table_names(db_url: str):
    engine = create_db_engine(db_url)
    inspector = inspect(engine)
    return inspector.get_table_names()
