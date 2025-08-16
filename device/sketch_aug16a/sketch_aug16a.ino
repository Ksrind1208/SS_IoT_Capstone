#include <WiFi.h>
#include <PubSubClient.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <time.h>

// ==== WiFi config ====
const char* ssid     = "Minh Tam 2.4 G";
const char* password = "21072018";

// ==== MQTT config ====
const char* mqtt_server = "192.168.1.44";
const int mqtt_port = 1883;
const char* mqtt_topic = "coldchain/fridge1/telemetry";

// ==== DS18B20 config ====
#define ONE_WIRE_BUS 4
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// ==== WiFi + MQTT client ====
WiFiClient espClient;
PubSubClient client(espClient);

// ==== NTP config ====
const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = 0;      // UTC
const int daylightOffset_sec = 0;  // No daylight saving

// Hàm kết nối Wi-Fi
void setup_wifi() {
  Serial.print("Kết nối Wi-Fi...");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWi-Fi đã kết nối, IP: " + WiFi.localIP().toString());
}

// Hàm kết nối MQTT
void reconnect() {
  while (!client.connected()) {
    Serial.print("Đang kết nối MQTT...");
    if (client.connect("ESP32Client")) {
      Serial.println("OK");
    } else {
      Serial.print("Lỗi, rc=");
      Serial.print(client.state());
      Serial.println(" thử lại sau 5s");
      delay(5000);
    }
  }
}

// Lấy thời gian UTC ISO 8601
String getISO8601Time() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) {
    Serial.println("Không lấy được thời gian");
    return "";
  }

  // Thêm offset +07:00
  time_t rawtime;
  time(&rawtime);
  rawtime += 7 * 3600; // cộng 7 giờ
  struct tm *timeinfo_offset = gmtime(&rawtime);

  char buffer[40];
  strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S+07:00", timeinfo_offset);
  return String(buffer);
}


void setup() {
  Serial.begin(115200);
  sensors.begin();

  setup_wifi();
  client.setServer(mqtt_server, mqtt_port);

  // Lấy thời gian từ NTP
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  // Đọc nhiệt độ
  sensors.requestTemperatures();
  float tempC = sensors.getTempCByIndex(0);

  if (tempC != DEVICE_DISCONNECTED_C) {
    String ts = getISO8601Time();
    String payload = "{\"device_id\":\"fridge1\",\"t_c\":";
    payload += String(tempC, 1);
    payload += ",\"ts\":\"" + ts + "\"}";

    Serial.println("Gửi MQTT: " + payload);
    client.publish(mqtt_topic, payload.c_str());
  } else {
    Serial.println("Không tìm thấy cảm biến!");
  }

  delay(5000); // gửi mỗi 5 giây
}
