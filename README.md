# GHI DỮ LIỆU AN TOÀN THỰC PHẨM CHO TỦ LẠNH CĂNG-TIN
Dự án COLDWATCH hướng tới số hóa quá trình giám sát này bằng IoT: cảm biến nhiệt độ DS18B20 gắn trong tủ lạnh kết nối ESP32 để đo và gửi dữ liệu thời gian thực; dữ liệu được truyền qua MQTT đến máy chủ, lưu vào SQLite và hiển thị qua Flask. Hệ thống không chỉ ghi nhận nhiệt độ liên tục mà còn tự động phát hiện sự kiện “vi phạm lạnh” (nhiệt độ > 4 °C kéo dài trên ngưỡng thời gian cấu hình, ví dụ 15 phút), từ đó lập bảng sự kiện, cảnh báo và cung cấp báo cáo/CSV cho quản sinh. Cách tiếp cận này giúp đảm bảo tuân thủ HACCP, minh bạch hóa hồ sơ nhiệt độ, và hỗ trợ phản ứng nhanh khi xảy ra bất thường (mở cửa tủ lâu, mất điện, thiết bị suy giảm hiệu năng).
## KIẾN TRÚC HỆ THỐNG
<img width="1559" height="623" alt="image" src="https://github.com/user-attachments/assets/026a2c16-467b-4f36-8d9e-47ca1dea516f" />

## Cách chạy
### 1. Phần cứng
Kết nối ESP32 và DS18B20 theo sơ đồ, cấp nguồn qua cổng USB và nạp code thông qua ArduinoIDE
<img width="750" height="816" alt="image" src="https://github.com/user-attachments/assets/1cb00827-b14b-42a2-a59d-6fc145f1ee46" />

### 2. Chạy MQTT Broker
```bash
sudo docker start mosquitto
```
### 3. Cài đặt và kích hoạt môi trường ảo Python
### Cài đặt
```bash
python3 -m venv env
```
### Kích hoạt
Đối với LINUX
```bash
source env/bin/active
```
Đối với Window
```bash
env\Scripts\activate
```
### 4. Chạy các file python bằng lệnh
```bash
python3 <tên_file>
```
## Kết quả
### ESP32 và DS18B20
<img width="846" height="195" alt="image" src="https://github.com/user-attachments/assets/48dbe88f-3f60-4620-8db4-8d9f43fe5d89" />

### Giao diện người dùng
Ngưỡng cảnh báo kiểm thử là 1 phút
#### Khi chưa vượt ngưỡng thời gian 
<img width="666" height="393" alt="image" src="https://github.com/user-attachments/assets/acbda138-3e53-49a9-b0be-919953fc7455" />

#### Khi vượt ngưỡng thời gian
<img width="671" height="401" alt="image" src="https://github.com/user-attachments/assets/caf6d040-2b18-430b-9bac-2265ee106f53" />

## Phát triển tương lai
-	Sử dụng cảm biến nhiệt độ chống nước, độ chính xác cao hơn và bổ sung thêm điểm đo để giám sát nhiều vị trí trong cùng một tủ lạnh.
-	Phát triển tính năng cảnh báo tức thời qua email/SMS khi phát hiện vi phạm lạnh.
-	Xây dựng ứng dụng di động hỗ trợ theo dõi và quản lý từ xa.
-	Mở rộng hệ thống để giám sát nhiều tủ lạnh/cơ sở cùng lúc với dữ liệu đồng bộ và quản lý tập trung. 
-	Tích hợp AI dùng để dự đoán xu hướng nhiệt độ, phát hiện bất thường hoặc tối ưu cảnh báo thay vì chỉ dựa vào ngưỡng cố định. 





