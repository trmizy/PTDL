from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, expr, trim,explode,split
from pyspark.sql.types import DoubleType,StringType,IntegerType
from pyspark.sql.functions import col, when,lower

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BookStore Data Cleaning") \
    .config("spark.mongodb.input.uri", "mongodb://mymongodb:27017/BookStore_Crawler_Full.BookList") \
    .config("spark.mongodb.output.uri", "mongodb://mymongodb:27017/BookStore_Crawler_Full.BookList") \
    .getOrCreate()

# Load data from MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()#

# Bước 1: Chuyển cột SKU về kiểu chuỗi trước
df_cleaned = df.withColumn("SKU", col("SKU").cast(StringType()))

# Bước 2: Bỏ chữ "SKU: " khỏi cột SKU
df_cleaned = df_cleaned.withColumn("SKU", regexp_replace(col("SKU"), "SKU: ", ""))

# Step 2: Remove 'Loại sản phẩm: ' from The_Loai column
df_cleaned = df_cleaned.withColumn("The_Loai", regexp_replace(col("The_Loai"), "Loại sản phẩm: ", ""))

# Step 3: Remove commas and '₫' from Gia column and convert it to numeric
df_cleaned = df_cleaned.withColumn("Gia", regexp_replace(col("Gia"), "[₫,]", "").cast(DoubleType()))

# Step 4: Replace empty strings in Ten_NXB and Ten_NCC with 'Unknown'
df_cleaned = df_cleaned.withColumn("Ten_NXB", when(col("Ten_NXB") == "", "Unknown").otherwise(col("Ten_NXB"))) \
                       .withColumn("Ten_NCC", when(col("Ten_NCC") == "", "Unknown").otherwise(col("Ten_NCC")))

# Step 5: Remove 'cm' from Kich_Thuoc
df_cleaned = df_cleaned.withColumn("Kich_Thuoc", regexp_replace(col("Kich_Thuoc"), " cm", ""))

# Step 4: Replace empty strings in specific columns with default values
df_cleaned = df_cleaned.withColumn("Trong_Luong", when(col("Trong_Luong") == "", "0").otherwise(col("Trong_Luong"))) \
    .withColumn("Kich_Thuoc", when(col("Kich_Thuoc") == "", "Unknown").otherwise(col("Kich_Thuoc"))) \
    .withColumn("So_Trang", when(col("So_Trang") == "", "0").otherwise(col("So_Trang"))) \
    .withColumn("Mo_Ta", when(col("Mo_Ta") == "", "Chưa có mô tả").otherwise(col("Mo_Ta"))) \
    .withColumn("Hinh_Thuc", when(col("Hinh_Thuc") == "", "Unknown").otherwise(col("Hinh_Thuc")))
# Thay thế giá trị trống trong "Ten_Tac_Gia" và "Nam_XB"
df_cleaned = df_cleaned.withColumn("Ten_Tac_Gia", when(col("Ten_Tac_Gia") == "", "Unknown").otherwise(col("Ten_Tac_Gia"))) \
                       .withColumn("Nam_XB", when(col("Nam_XB") == "", "Unknown").otherwise(col("Nam_XB")))

# Chuẩn hóa các giá trị trong cột "The_Loai" về "Sách" cho các trường hợp "SÁCH", "sách", "Sách"
df_cleaned = df_cleaned.withColumn(
    "The_Loai",
    when(lower(col("The_Loai")).isin("sách"), "Sách").otherwise(col("The_Loai"))
)
# Bước 1: Chuyển đổi cột "Ten_Tac_Gia" thành chuỗi
df_cleaned = df_cleaned.withColumn("Ten_Tac_Gia", col("Ten_Tac_Gia").cast("string"))
# Đếm số lượng bản ghi theo SKU
duplicate_skus = df_cleaned.groupBy("SKU").count().filter(col("count") > 1).select("SKU")

# Lấy danh sách SKU bị trùng
duplicate_sku_list = [row.SKU for row in duplicate_skus.collect()]

# Lọc ra các bản ghi không có trong danh sách SKU bị trùng
df_cleaned = df_cleaned.filter(~col("SKU").isin(duplicate_sku_list))
# Bước 2: Sửa lại các giá trị trong cột "Ten_Tac_Gia"
# Sửa "Thu Giang Nguyễn Duy Cần" thành "Thu Giang, Nguyễn Duy Cần"
df_cleaned = df_cleaned.withColumn(
    "Ten_Tac_Gia", 
    regexp_replace(col("Ten_Tac_Gia"), "Thu Giang Nguyễn Duy Cần", "Thu Giang, Nguyễn Duy Cần")
)
#Chuyển dổi kiểu dữ liệu theo mô hình hóa dữ liệu
df_cleaned = df_cleaned.withColumn("Nam_XB", col("Nam_XB").cast(IntegerType())) \
                       .withColumn("Trong_Luong", regexp_replace(col("Trong_Luong"), "[grg]", "").cast(IntegerType())) \
                       .withColumn("So_Trang", col("So_Trang").cast(IntegerType()))


#########################POSTGRES##############################
# Bước 9: Tách "Ten_Tac_Gia" thành các giá trị riêng biệt
df_split_authors = df_cleaned.withColumn("author", explode(split(col("Ten_Tac_Gia"), "[,-]"))) \
    .select(trim(col("author")).alias("author_name")).distinct()

# PostgreSQL connection details
postgres_url = "jdbc:postgresql://mypostgres:5432/bookstore_db"
postgres_properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

# Ghi danh sách tác giả vào bảng 'tac_gia'
df_split_authors.write.jdbc(
    url=postgres_url,
    table="tac_gia",
    mode="append",  # Chỉ ghi các bản ghi không trùng lặp
    properties=postgres_properties
)

# Đọc lại bảng 'tac_gia' để lấy ID
authors_with_id_df = spark.read.jdbc(postgres_url, "tac_gia", properties=postgres_properties)

# Bước 11: Ghi danh sách Nhà Xuất Bản vào PostgreSQL
publishers_df = df_cleaned.select(trim(col("Ten_NXB")).alias("ten_nxb")).distinct()
publishers_df.write.jdbc(
    url=postgres_url,
    table="nha_xuat_ban",
    mode="append",  # Chỉ thêm bản ghi mới
    properties=postgres_properties
)

# Đọc lại bảng 'nha_xuat_ban' để lấy id nhà xuất bản
publishers_with_id_df = spark.read.jdbc(postgres_url, "nha_xuat_ban", properties=postgres_properties)

# Bước 12: Ghi danh sách Nhà Cung Cấp vào PostgreSQL
suppliers_df = df_cleaned.select(trim(col("Ten_NCC")).alias("ten_ncc")).distinct()
suppliers_df.write.jdbc(
    url=postgres_url,
    table="nha_cung_cap",
    mode="append",  # Chỉ thêm bản ghi mới
    properties=postgres_properties
)

# Đọc lại bảng 'nha_cung_cap' để lấy id nhà cung cấp
suppliers_with_id_df = spark.read.jdbc(postgres_url, "nha_cung_cap", properties=postgres_properties)

# Bước 13: Ánh xạ các tác giả với sách thông qua SKU
df_with_author_ids = df_cleaned.withColumn("author_array", split(col("Ten_Tac_Gia"), "[,-]")) \
    .select("SKU", explode(col("author_array")).alias("cleaned_author_name"))

df_author_mapping = df_with_author_ids.join(
    authors_with_id_df.select("author_id", "author_name"),
    trim(col("cleaned_author_name")) == trim(authors_with_id_df.author_name),
    "left"
).groupBy("SKU").agg(expr("collect_list(author_id) as author_ids")).distinct()

# Bước 14: Chuẩn bị dữ liệu cho bảng 'sach'
# Chuẩn bị dữ liệu cho bảng 'sach'
df_books = df_cleaned.join(
    publishers_with_id_df.withColumnRenamed("id", "nxb_id"),  # Renaming to nxb_id
    df_cleaned.Ten_NXB == publishers_with_id_df.ten_nxb,
    "left"
).join(
    suppliers_with_id_df.withColumnRenamed("id", "ncc_id"),  # Renaming to ncc_id
    df_cleaned.Ten_NCC == suppliers_with_id_df.ten_ncc,
    "left"
).join(
    df_author_mapping,
    "SKU"
).select(
    "SKU", "Ten_Sach", "Gia", "Nam_XB", "Kich_Thuoc", "Trong_Luong", "So_Trang",
    "Hinh_Thuc", "Mo_Ta", "nxb_id", "ncc_id", "author_ids"
)
df_books = df_books.withColumn(
    "so_trang", 
    when(col("so_trang").isNull() | (col("so_trang") == ""), 0).otherwise(col("so_trang").cast(IntegerType()))
)
# Đọc lại bảng 'sach' để lấy tất cả các SKU đã có
existing_sku_df = spark.read.jdbc(postgres_url, "sach", properties=postgres_properties)

# Lọc bỏ các bản ghi có SKU đã tồn tại trong bảng 'sach'
df_books_filtered = df_books.join(existing_sku_df, "SKU", "left_anti")

# In ra một số bản ghi đã lọc
df_books_filtered.show(truncate=False)

# Ghi bảng 'sach' vào PostgreSQL
df_books_filtered.write.jdbc(
    url=postgres_url,
    table="sach",
    mode="append",  # Chỉ thêm bản ghi mới không bị trùng lặp
    properties=postgres_properties
)
df_author_mapping1 = df_books.select("SKU", explode(col("author_ids")).alias("author_id"))

# Lưu dữ liệu vào bảng sach_tac_gia trong PostgreSQL
df_cleaned = df_author_mapping1.dropDuplicates(["SKU", "author_id"]) 
df_cleaned.write.jdbc(
    url=postgres_url,
    table="sach_tac_gia",
    mode="append",  # Bỏ qua những bản ghi đã tồn tại
    properties=postgres_properties
)

print("Dữ liệu đã được ghi vào PostgreSQL.")