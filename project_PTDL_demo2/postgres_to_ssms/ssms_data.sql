-- Tạo cơ sở dữ liệu
CREATE DATABASE Data_book_1;
go
-- Sử dụng cơ sở dữ liệu mới tạo
USE Data_book_1;

-- Tạo bảng Tac_Gia

CREATE TABLE tac_gia 
(
    author_id INT PRIMARY KEY,
    author_name NVARCHAR(255)
);

-- Tạo bảng Nha_Cung_Cap
CREATE TABLE nha_cung_cap 
(
    id INT PRIMARY KEY,
    ten_ncc NVARCHAR(255)
);

-- Tạo bảng Nha_Xuat_Ban
CREATE TABLE nha_xuat_ban 
(
    id INT PRIMARY KEY,
    ten_nxb NVARCHAR(255) 
);

-- Tạo bảng The_Loai
CREATE TABLE the_loai 
(
    the_loai_id INT PRIMARY KEY,
    ten_the_loai NVARCHAR(255)
);

-- Tạo bảng Sach
CREATE TABLE sach 
(
    sku NVARCHAR(255) PRIMARY KEY,
    ten_sach NVARCHAR(255),
    gia_moi FLOAT,
	gia_cu FLOAT,
    giam_gia FLOAT,
    rating FLOAT,
    review_count INT,
    nam_xb NVARCHAR(255),
    kich_thuoc NVARCHAR(255),
    trong_luong NVARCHAR(255),
    so_trang INT,
    hinh_thuc NVARCHAR(255),
    mo_ta NVARCHAR(MAX), 
    nxb_id INT FOREIGN KEY REFERENCES nha_xuat_ban(id),
    ncc_id INT FOREIGN KEY REFERENCES nha_cung_cap(id),
    tl_id INT FOREIGN KEY REFERENCES the_loai(the_loai_id)
);

-- Tạo bảng Sach_Tac_Gia
CREATE TABLE sach_tac_gia (
    sku NVARCHAR(255),
    author_id INT,
    PRIMARY KEY (sku, author_id),
    FOREIGN KEY (sku) REFERENCES sach(sku),
    FOREIGN KEY (author_id) REFERENCES tac_gia(author_id)
);
