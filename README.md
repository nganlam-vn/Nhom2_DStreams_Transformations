# Nhom2_DStreams_Transformations
Demo và Bài tập về nhà của nội dung Các phép biến đổi trên DStreams. 

## **HƯỚNG DẪN CHẠY** 

### **Bước 1: Chuẩn bị và Khởi chạy Môi trường**
1.	Đảm bảo Spark đã dược cài đặt trên máy
____________________________________
### **Bước 2: Dùng file logs có sẵn và lấy ngẫu nhiên các logs để khởi tạo streaming logs**
Producer sẽ giả lập các logs và gửi liên tục 
1.	Mở terminal, truy cập vào thư mục có chứa script send_logs.py và chạy:
```bash
python send_logs.py
```
<img width="945" height="250" alt="image" src="https://github.com/user-attachments/assets/7e7f5849-3ccf-4b15-a278-f804f6099489" />

________________________________________
### **Bước 3: Khởi tạo chương trình **
1.	Khởi tạo chương trình filter và map tương ứng để trích xuất một thông tin cụ thể (ví dụ: ip của người dùng):
```bash
python log_analyzer_exercise
```
<img width="945" height="485" alt="image" src="https://github.com/user-attachments/assets/c714a6b6-c082-47c3-8c69-fbd0b7467f2f" />
