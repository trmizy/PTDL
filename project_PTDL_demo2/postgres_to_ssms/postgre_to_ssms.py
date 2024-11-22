import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy

# Kết nối PostgreSQL
def connect_postgres():
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "bookstore_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "12345")
        )
        print("Kết nối với PostgreSQL thành công.")
        return pg_conn
    except Exception as e:
        print(f"Không thể kết nối tới PostgreSQL: {e}")
        return None

# Trích xuất dữ liệu từ PostgreSQL
def fetch_data_from_postgres(query, conn):
    try:
        df = pd.read_sql(query, conn)
        print(f"Dữ liệu được trích xuất thành công từ truy vấn: {query[:50]}...")
        return df
    except Exception as e:
        print(f"Lỗi khi trích xuất dữ liệu từ PostgreSQL: {e}")
        return None

# Kết nối SQL Server
def connect_mssql():
    try:
        mssql_engine = create_engine(
            f"mssql+pyodbc://{os.getenv('MSSQL_USER', 'sa')}:{os.getenv('MSSQL_PASSWORD', 'YourPassword123')}@"
            f"{os.getenv('MSSQL_HOST', 'localhost')}:{os.getenv('MSSQL_PORT', '1433')}/
            {os.getenv('MSSQL_DB', 'Data_book_1')}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        print("Kết nối với SQL Server thành công.")
        return mssql_engine
    except Exception as e:
        print(f"Không thể kết nối tới SQL Server: {e}")
        return None

# Tải dữ liệu vào SQL Server
def load_data_to_mssql(df, table_name, engine):
    try:
        dtype_mapping = {}

        # Điều chỉnh kiểu NVARCHAR cho các cột cụ thể
        if table_name == 'tac_gia':
            dtype_mapping = {'author_name': sqlalchemy.types.NVARCHAR(length=255)}
        elif table_name == 'nha_cung_cap':
            dtype_mapping = {'ten_ncc': sqlalchemy.types.NVARCHAR(length=255)}
        elif table_name == 'nha_xuat_ban':
            dtype_mapping = {'ten_nxb': sqlalchemy.types.NVARCHAR(length=255)}
        elif table_name == 'the_loai':
            dtype_mapping = {'ten_the_loai': sqlalchemy.types.NVARCHAR(length=255)}
        elif table_name == 'sach':
            dtype_mapping = {
                'sku': sqlalchemy.types.NVARCHAR(length=255),
                'ten_sach': sqlalchemy.types.NVARCHAR(length=255),
                'nam_xb': sqlalchemy.types.NVARCHAR(length=255),
                'kich_thuoc': sqlalchemy.types.NVARCHAR(length=255),
                'trong_luong': sqlalchemy.types.NVARCHAR(length=255),
                'hinh_thuc': sqlalchemy.types.NVARCHAR(length=255),
                'mo_ta': sqlalchemy.types.NVARCHAR(length='max')
            }

        # Lấy dữ liệu đã tồn tại trong bảng
        existing_data = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

        # Loại bỏ dữ liệu trùng lặp trước khi chèn
        if table_name == 'tac_gia':
            df = df[~df['author_name'].isin(existing_data['author_name'])]
        elif table_name == 'nha_cung_cap':
            df = df[~df['ten_ncc'].isin(existing_data['ten_ncc'])]
        elif table_name == 'nha_xuat_ban':
            df = df[~df['ten_nxb'].isin(existing_data['ten_nxb'])]
        elif table_name == 'the_loai':
            df = df[~df['ten_the_loai'].isin(existing_data['ten_the_loai'])]
        elif table_name == 'sach':
            df = df[~df['sku'].isin(existing_data['sku'])]
        elif table_name == 'sach_tac_gia':
            df = df[~df.set_index(['sku', 'author_id']).index.isin(
                existing_data.set_index(['sku', 'author_id']).index
            )]

        # Chèn dữ liệu mới
        if not df.empty:
            df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping)
            print(f"Dữ liệu đã được tải thành công vào bảng {table_name}.")
        else:
            print(f"Không có dữ liệu mới để tải vào bảng {table_name}.")
    except Exception as e:
        print(f"Lỗi khi tải dữ liệu vào bảng {table_name}: {e}")

# Hàm chính
def main():
    # Kết nối PostgreSQL
    pg_conn = connect_postgres()
    if pg_conn is None:
        return

    # Trích xuất dữ liệu
    cleaned_data_df = fetch_data_from_postgres("SELECT * FROM CleanedData;", pg_conn)
    pg_conn.close()

    if cleaned_data_df is None or cleaned_data_df.empty:
        print("Dữ liệu không tồn tại hoặc rỗng.")
        return

    # Kết nối SQL Server
    mssql_engine = connect_mssql()
    if mssql_engine is None:
        return

    # Chuẩn bị dữ liệu
    # Lọc và tạo ID cho DataFrame tac_gia
    tac_gia_df = cleaned_data_df[['ten_tac_gia']].drop_duplicates().rename(columns={'ten_tac_gia': 'author_name'})
    tac_gia_df = tac_gia_df.reset_index(drop=True)
    tac_gia_df['author_id'] = tac_gia_df.index + 1

    # Lọc và tạo ID cho DataFrame nha_cung_cap
    ncc_df = cleaned_data_df[['ten_ncc']].drop_duplicates().rename(columns={'ten_ncc': 'ten_ncc'})
    ncc_df = ncc_df.reset_index(drop=True)
    ncc_df['id'] = ncc_df.index + 1

    # Lọc và tạo ID cho DataFrame nha_xuat_ban
    nxb_df = cleaned_data_df[['ten_nxb']].drop_duplicates().rename(columns={'ten_nxb': 'ten_nxb'})
    nxb_df = nxb_df.reset_index(drop=True)
    nxb_df['id'] = nxb_df.index + 1

    # Lọc và tạo ID cho DataFrame the_loai
    the_loai_df = cleaned_data_df[['the_loai']].drop_duplicates().rename(columns={'the_loai': 'ten_the_loai'})
    the_loai_df = the_loai_df.reset_index(drop=True)
    the_loai_df['the_loai_id'] = the_loai_df.index + 1

    sach_df = cleaned_data_df[['sku', 'ten_sach', 'gia_cu', 'gia_moi', 'giam_gia', 'rating', 'review_count', 
                               'nam_xb', 'kich_thuoc', 'trong_luong', 'so_trang', 'hinh_thuc', 'mo_ta']].copy()
    sach_df['nxb_id'] = cleaned_data_df['ten_nxb'].map(nxb_df.set_index('ten_nxb')['id'])
    sach_df['ncc_id'] = cleaned_data_df['ten_ncc'].map(ncc_df.set_index('ten_ncc')['id'])
    sach_df['tl_id'] = cleaned_data_df['the_loai'].map(the_loai_df.set_index('ten_the_loai')['the_loai_id'])

    sach_tac_gia_df = cleaned_data_df[['sku', 'ten_tac_gia']].copy()
    sach_tac_gia_df['author_id'] = sach_tac_gia_df['ten_tac_gia'].map(
        tac_gia_df.set_index('author_name')['author_id']
    )
    sach_tac_gia_df = sach_tac_gia_df[['sku', 'author_id']].dropna().drop_duplicates()
    sach_tac_gia_df['author_id'] = sach_tac_gia_df['author_id'].astype(int)

    # Tải dữ liệu vào SQL Server
    load_data_to_mssql(tac_gia_df, 'tac_gia', mssql_engine)
    load_data_to_mssql(ncc_df, 'nha_cung_cap', mssql_engine)
    load_data_to_mssql(nxb_df, 'nha_xuat_ban', mssql_engine)
    load_data_to_mssql(the_loai_df, 'the_loai', mssql_engine)
    load_data_to_mssql(sach_df, 'sach', mssql_engine)
    load_data_to_mssql(sach_tac_gia_df, 'sach_tac_gia', mssql_engine)

    # Ngắt kết nối
    mssql_engine.dispose()
    print("Kết nối SQL Server đã được đóng.")

if __name__ == "__main__":
    main()
