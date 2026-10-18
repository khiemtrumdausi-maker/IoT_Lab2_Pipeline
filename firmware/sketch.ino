// Bai 2 - Thu thap du lieu IoT: ESP32 (Wokwi) -> MQTT broker cong cong -> Python collector -> InfluxDB
// Giu nguyen mach Bai 1 (DHT22 GPIO15, HC-SR04 TRIG5/ECHO18, LED GPIO2).
// Khac Bai 1: gui len broker chung (khong dung token ThingsBoard), them device_id + ts (NTP, ms) de do do tre.
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <time.h>
#include <sys/time.h>

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

const char* MQTT_SERVER = "broker.hivemq.com";   // co the doi: broker.emqx.io / test.mosquitto.org
const int   MQTT_PORT = 1883;
const char* DEVICE_ID = "b23dccn435-esp32-01";   // <-- doi thanh MSSV cua ban (viet thuong)
// Topic chuan: iot/<mssv>/<bai>/<device>/<loai>
const char* TOPIC_TELEMETRY = "iot/b23dccn435/lab2/esp32-01/telemetry";
const char* TOPIC_STATUS    = "iot/b23dccn435/lab2/esp32-01/status";

const int DHT_PIN = 15, TRIG_PIN = 5, ECHO_PIN = 18, LED_PIN = 2;
const unsigned long SEND_INTERVAL_MS = 5000;

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
DHTesp dht;
unsigned long lastSend = 0, sequenceNo = 0;

void connectWiFi() {
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  Serial.print("Connecting WiFi");
  while (WiFi.status() != WL_CONNECTED) { delay(500); Serial.print("."); }
  Serial.println(" connected");
}

void syncTime() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.print("Sync NTP");
  while (time(nullptr) < 1700000000) { delay(300); Serial.print("."); }
  Serial.println(" ok");
}

uint64_t nowMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

void connectMQTT() {
  while (!mqttClient.connected()) {
    String clientId = String("ESP32-Lab2-") + String((uint32_t)ESP.getEfuseMac(), HEX);
    Serial.print("Connecting MQTT...");
    // Last Will: broker tu dong bao "offline" neu thiet bi mat ket noi dot ngot
    if (mqttClient.connect(clientId.c_str(), nullptr, nullptr, TOPIC_STATUS, 1, true, "offline")) {
      Serial.println(" connected");
      mqttClient.publish(TOPIC_STATUS, "online", true);
    } else {
      Serial.printf(" failed, rc=%d. Retry in 2 s\n", mqttClient.state());
      delay(2000);
    }
  }
}

float readDistanceCm() {
  digitalWrite(TRIG_PIN, LOW);  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH); delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  unsigned long duration = pulseIn(ECHO_PIN, HIGH, 30000);
  if (duration == 0) return NAN;
  return duration * 0.0343f / 2.0f;
}

void setup() {
  Serial.begin(115200);
  pinMode(TRIG_PIN, OUTPUT); pinMode(ECHO_PIN, INPUT); pinMode(LED_PIN, OUTPUT);
  dht.setup(DHT_PIN, DHTesp::DHT22);
  connectWiFi();
  syncTime();
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  mqttClient.setBufferSize(384);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!mqttClient.connected()) connectMQTT();
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastSend < SEND_INTERVAL_MS) return;
  lastSend = now;

  TempAndHumidity d = dht.getTempAndHumidity();
  float dist = readDistanceCm();
  if (!isfinite(d.temperature) || !isfinite(d.humidity) || !isfinite(dist)) {
    Serial.println("Invalid sensor data - skip publish");
    return;
  }

  sequenceNo++;
  char payload[320];
  snprintf(payload, sizeof(payload),
    "{\"device_id\":\"%s\",\"ts\":%llu,\"sequence\":%lu,\"uptime_s\":%lu,"
    "\"temperature\":%.2f,\"humidity\":%.2f,\"distance_cm\":%.2f,\"rssi\":%d}",
    DEVICE_ID, (unsigned long long)nowMs(), sequenceNo, millis() / 1000,
    d.temperature, d.humidity, dist, WiFi.RSSI());

  bool ok = mqttClient.publish(TOPIC_TELEMETRY, payload);
  Serial.printf("%s | publish=%s\n", payload, ok ? "OK" : "FAILED");
  digitalWrite(LED_PIN, HIGH); delay(80); digitalWrite(LED_PIN, LOW);
}
