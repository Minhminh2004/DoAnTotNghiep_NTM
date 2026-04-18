CREATE DATABASE DATN_NTM;
GO

CREATE TABLE dbo.KHOA (
    MaKhoa INT PRIMARY KEY,
    TenKhoa NVARCHAR(100) NOT NULL,
    NgayThanhLap DATE NOT NULL,
    DiemChuan FLOAT NOT NULL
);
GO

CREATE TABLE dbo.SINHVIEN (
    MaSV INT PRIMARY KEY,
    HoTen NVARCHAR(100) NOT NULL,
    NgaySinh DATE NOT NULL,
    GioiTinh NVARCHAR(10) NOT NULL,
    Email NVARCHAR(100) NULL,
    SoDienThoai NVARCHAR(20) NULL,
    DiaChi NVARCHAR(200) NULL,
    Khoa NVARCHAR(100) NULL,
    Lop NVARCHAR(50) NULL,
    MaKhoa INT NOT NULL,

    CONSTRAINT FK_SINHVIEN_KHOA
        FOREIGN KEY (MaKhoa) REFERENCES dbo.KHOA(MaKhoa)
);
GO

INSERT INTO dbo.KHOA (MaKhoa, TenKhoa, NgayThanhLap, DiemChuan)
VALUES
(1, N'Công nghệ thông tin', '2010-09-01', 24.5),
(2, N'Kinh tế', '2008-08-15', 22.0),
(3, N'Quản trị kinh doanh', '2009-07-20', 23.0);
GO

INSERT INTO dbo.SINHVIEN (
    MaSV, HoTen, NgaySinh, GioiTinh, Email, SoDienThoai, DiaChi, Khoa, Lop, MaKhoa
)
VALUES
(1, N'Nguyễn Văn An', '2002-03-15', N'Nam', N'an.nguyen@example.com', N'0912345678', N'Hà Nội', N'Công nghệ thông tin', N'CNTT01', 1),
(2, N'Trần Thị Bình', '2001-07-22', N'Nữ', N'binh.tran@example.com', N'0923456789', N'Hải Phòng', N'Kinh tế', N'KT02', 2),
(3, N'Lê Văn Cường', '2003-01-10', N'Nam', N'cuong.le@example.com', N'0934567890', N'Đà Nẵng', N'Quản trị kinh doanh', N'QTKD03', 3);
GO

SELECT * FROM dbo.KHOA;
SELECT * FROM dbo.SINHVIEN;
GO


CREATE TABLE dbo.SANPHAM (
    MaSP INT PRIMARY KEY,
    TenSP NVARCHAR(100) NOT NULL,
    NgayNhap DATE NOT NULL,
    Gia FLOAT NOT NULL
);
GO

INSERT INTO dbo.SANPHAM (MaSP, TenSP, NgayNhap, Gia)
VALUES
(1, N'Laptop Dell', '2023-01-10', 15000000),
(2, N'Chuột Logitech', '2023-02-15', 500000);
GO

SELECT * FROM dbo.SANPHAM;
GO