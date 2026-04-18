
CREATE TABLE sanpham (
    masp INT PRIMARY KEY,
    tensp VARCHAR(100),
    ngaynhap DATE,
    gia FLOAT
);
INSERT INTO sanpham (masp, tensp, ngaynhap, gia)
VALUES
(1, 'Laptop Dell', '2023-01-10', 15000000),
(2, 'Chuột Logitech', '2023-02-15', 500000);
SELECT * FROM sanpham;