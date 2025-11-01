# Nhom2_DStreams_Transformations

Demo và Bài tập về nhà của nội dung Các phép biến đổi trên DStreams.

## **HƯỚNG DẪN CHẠY**

### **Bước 1: Chuẩn bị và Khởi chạy Môi trường**

1. Đảm bảo Spark đã được cài đặt trên máy
2. Đảm bảo Python đã được cài đặt với các thư viện cần thiết:
   - pyspark
   - logging

## **DEMO 1: Phép biến đổi cơ bản trên DStreams**

Thư mục: `transformation_dstream_demo`

### **Bước 1: Khởi động Producer**

1. Mở terminal thứ nhất
2. Di chuyển vào thư mục `transformation_dstream_demo`
3. Chạy script để gửi logs:

```bash
python send_logs.py
```

### **Bước 2: Chạy Log Analyzer**

1. Mở terminal thứ hai
2. Di chuyển vào thư mục `transformation_dstream_demo`
3. Chạy script phân tích logs:

```bash
python log_analyzer.py
```

Chương trình sẽ:

- Lọc các log có mức "ERROR"
- Chuyển đổi nội dung log thành chữ hoa
- Lưu các ERROR gốc vào file `raw_errors.txt`
- Hiển thị kết quả đã chuẩn hóa theo từng batch

<img width="1918" height="1138" alt="image" src="https://github.com/user-attachments/assets/a01df8dc-1189-4aa9-ac41-271ff2a9bf90" />


## **DEMO 2: Các phép biến đổi phức tạp**

Thư mục: `transform_complex_case`

### **Bước 1: Khởi động Producer**

1. Mở terminal thứ nhất
2. Di chuyển vào thư mục `transform_complex_case`
3. Chạy script để gửi logs:

```bash
python send_logs.py
```

### **Bước 2: Chạy Log Analyzer**

1. Mở terminal thứ hai
2. Di chuyển vào thư mục `transform_complex_case`
3. Chạy script phân tích logs:

```bash
python log_analyzer_case.py
```

Chương trình có 2 tính năng (điều chỉnh bằng biến flag):

- ENABLE_DEMO_1 = 1: Lọc message trùng lặp, chỉ giữ lại 1 bản ghi
  <img width="1919" height="1138" alt="image" src="https://github.com/user-attachments/assets/343ff33e-f6a8-4831-91f6-40e69390dcd6" />

- ENABLE_DEMO_2 = 1: Đánh dấu và hiển thị log có chứa IP/user trong blacklist
<img width="1918" height="1137" alt="image" src="https://github.com/user-attachments/assets/79f43117-61d7-4575-9019-28bcec2a9352" />

## **BÀI TẬP**

Thư mục: `exercises`

### **Bước 1: Khởi động Producer**

1. Mở terminal thứ nhất
2. Di chuyển vào thư mục `exercises`
3. Chạy script để gửi logs:

```bash
python send_logs.py
```

Producer sẽ bắt đầu gửi logs liên tục:

<img width="945" height="250" alt="Producer sending logs" src="https://github.com/user-attachments/assets/7e7f5849-3ccf-4b15-a278-f804f6099489" />

### **Bước 2: Chạy Log Analyzer**

1. Mở terminal thứ hai
2. Di chuyển vào thư mục `exercises`
3. Khởi tạo chương trình filter và map để trích xuất thông tin cụ thể (ví dụ: ip của người dùng):

```bash
python log_analyzer_exercise.py
```

Kết quả phân tích sẽ được hiển thị như sau:

<img width="945" height="485" alt="Log analysis results" src="https://github.com/user-attachments/assets/c714a6b6-c082-47c3-8c69-fbd0b7467f2f" />

File logs mẫu được cung cấp trong `sample_logs_exercise.txt`
