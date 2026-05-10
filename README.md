# Real-Time Fraud Detection for P2P Payments

> Projekt na przedmiot **Analiza Danych w Czasie Rzeczywistym** - SGH 2025/26

---

## 
haslo do jupytera to rta
## 1. Problem biznesowy

Platformy platnosci P2P (BLIK-to-BLIK, Revolut, PayPal Friends) staja przed problemem
oszustw finansowych dzielacych sie na kilka wzorcow:

| Wzorzec | Opis |
|---|---|
| **Account Takeover** | Przestepca loguje sie z obcego urzadzenia / innego kraju i zleca przelew. |
| **Rapid-fire / smurfing** | Wiele malych przelewow w krotkim czasie, by objsc limity alertow. |
| **Anomalia geolokalizacyjna** | Transakcja z miejsca bardzo odleglego od miasta rejestracji. |
| **Round-trip** | Srodki odsylane natychmiast miedzy dwoma kontami, by zamaskowac skradziona kwote. |
| **Layering** | Srodki przechodza przez lancuch kont-slupow, by utrudnic sledzenie. |

**Cel systemu:** ocenic ryzyko kazdej transakcji w czasie < 200 ms i zwrocic wynik
(APPROVE / REVIEW / BLOCK) zanim bank zatwierdzi przelew.

---

## 2. Architektura systemu

```
generator.py  -->  Kafka (topics: transactions, app_events)  -->  Spark Streaming  -->  Kafka (topic: alerts)  -->  Grafana
```

Szczegolowo:

```
+------------------------------------------------------------------+
|                        DATA GENERATOR                            |
|  profiles.py --> event_builder.py --> fraud_scenarios.py         |
|                       generator.py                               |
+------------------------------+-----------------------------------+
                               | JSON / kafka-python-ng
               +---------------v--------------+
               |         Apache Kafka          |
               |  topic: transactions          |
               |  topic: app_events            |
               +------+------------------+-----+
                      |                  |
         +------------v------------------v-----+
         |      Apache Spark Structured        |
         |      Streaming  (job Pythonowy)      |
         |  - parsowanie JSON                  |
         |  - okna czasowe (5 min, 1 h)        |
         |  - feature engineering              |
         |  - scoring / reguły decyzyjne       |
         +-------------------+-----------------+
                             |
               +-------------v--------------+
               |   Kafka topic: alerts       |
               |  { tx_id, risk_score,       |
               |    decision, fraud_type }   |
               +-------------+--------------+
                             |
               +-------------v--------------+
               |         Grafana            |
               |   dashboard real-time      |
               +----------------------------+
```

---

## 3. Status realizacji

| Krok | Komponent | Status |
|---|---|---|
| 1 | Silnik generowania danych (`data_generator/`) | **GOTOWE** |
| 2 | Infrastruktura Kafka (Docker Compose) | **GOTOWE** |
| 3 | Weryfikacja polaczenia generator -> Kafka | **GOTOWE** |
| 4 | Spark Structured Streaming job | TODO |
| 5 | Feature engineering (okna czasowe) | TODO |
| 6 | Model ML / reguły scoringowe | TODO |
| 7 | Dashboard Grafana | TODO |

---

## 4. Zrealizowane komponenty

### 4.1 `data_generator/` - silnik generowania danych

```
data_generator/
    profiles.py          # pula uzytkownikow: normal / mule / fraudster
    event_builder.py     # budowanie payloadow JSON dla obu tematow Kafka
    fraud_scenarios.py   # 5 wzorcow oszustw jako funkcje mutujace payload
    generator.py         # glowna petla + CLI + integracja z Kafka
    verify_kafka.py      # konsument weryfikacyjny - czyta wiadomosci z Kafki
    requirements.txt
```

**Populacja uzytkownikow (profiles.py):**
- normal (90%) - realistyczne parametry, polskie/europejskie miasta z GPS
- mule (7%)    - mlode konta, duzo transakcji, posrednicy w layeringu
- fraudster (3%) - bardzo nowe konta, wysokie kwoty

**Schematy wiadomosci:**

Temat `transactions`:
```json
{
  "tx_id": "uuid",
  "sender_id": "uuid",
  "recipient_id": "uuid",
  "amount": 345.94,
  "currency": "PLN",
  "device_id": "uuid",
  "device_type": "ios",
  "device_trusted": true,
  "sender_ip": "192.168.x.x",
  "sender_lat": 52.33,
  "sender_lon": 21.08,
  "sender_city": "Warsaw",
  "timestamp": "2026-04-20T12:17:37+00:00",
  "is_fraud": false,
  "fraud_type": "",
  "sender_account_age_days": 480,
  "sender_monthly_tx_count": 17,
  "sender_avg_amount": 293.92
}
```

Temat `app_events`:
```json
{
  "event_id": "uuid",
  "user_id": "uuid",
  "tx_id": "uuid",
  "timestamp": "2026-04-20T12:17:37+00:00",
  "pin_failures": 0,
  "device_changed": false,
  "new_device_id": "",
  "session_duration_sec": 119,
  "app_version": "3.7.3"
}
```

**Scenariusze oszustw (fraud_scenarios.py):**

| Scenariusz | Waga | Mutacje |
|---|---|---|
| account_takeover | 30% | obcy GPS+IP, device_trusted=False, off-hours |
| rapid_fire | 25% | kwota 5-50 PLN, timestampy co 3 s |
| geo_anomaly | 20% | GPS z innego kraju, obcy IP |
| round_trip | 15% | recipient_id = oryginalny nadawca |
| layering | 10% | recipient z puli mule, kwota -15% |

### 4.2 `docker-compose.yml` - infrastruktura Kafka

Uruchamia dwa kontenery:
- **rta_kafka** (`apache/kafka:latest`) - broker KRaft (bez Zookeepera), port 9092
- **rta_kafka_ui** (`provectuslabs/kafka-ui`) - interfejs webowy, port 8080

Tematy tworzone sa automatycznie przy pierwszej publikacji (`AUTO_CREATE_TOPICS_ENABLE=true`).

---

## 5. Uruchomienie

### Krok 1 - Start Kafki

```bash
docker compose up -d
# Poczekaj az kafka przejdzie w stan healthy (~30 s)
docker ps  # kolumna STATUS powinna pokazac "(healthy)"
```

### Krok 2 - Uruchomienie generatora

```bash
pip install -r data_generator/requirements.txt

# Wyslij dane do Kafki (10 tx/s, 10% fraudow)
python data_generator/generator.py --rate 10 --fraud 0.10

# Test bez Kafki (drukuje JSON na stdout)
python data_generator/generator.py --dry-run --rate 5
```

### Krok 3 - Weryfikacja odczytu z Kafki

```bash
python data_generator/verify_kafka.py --n 10
```

### Interfejs webowy Kafki

Otwórz w przegladarce: http://localhost:8080

---

## 6. Nastepne kroki

### Krok 4 - Apache Spark Structured Streaming

Napisac job Pythonowy (`spark_job/fraud_detector.py`) ktory:
1. Odczytuje strumien z tematu `transactions` i `app_events`
2. Parsuje JSON i joinuje oba strumienie po `tx_id`
3. Oblicza cechy w oknach czasowych:
   - `tx_count_last_5min` per `sender_id`
   - `amount_zscore` (odchylenie od historycznej sredniej)
   - `unique_recipients_1h`
   - `geo_distance_km` (dystans GPS od miasta rejestracji)
   - `pin_failure_rate`
4. Zwraca wynik jako JSON na temat `alerts`

Uruchomienie przez Docker (obraz `bitnami/spark` lub lokalny `spark-submit`).

### Krok 5 - Model ML

Trening offline (Isolation Forest / XGBoost) na danych z generatora (pole `is_fraud` jako etykieta).
Wczytanie modelu do Spark joba i scoring online.

Progi decyzyjne:
- APPROVE  : risk_score < 0.3
- REVIEW   : risk_score 0.3 - 0.7
- BLOCK    : risk_score > 0.7

### Krok 6 - Dashboard Grafana

Grafana + plugin Kafka -> wizualizacja real-time:
- throughput (tx/s)
- fraud rate (%)
- rozklad scenariuszy oszustw
- mapa GPS anomalii
