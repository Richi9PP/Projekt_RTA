# Real-Time Fraud Detection for P2P Payments

> Projekt na przedmiot **Analiza Danych w Czasie Rzeczywistym** - SGH 2025/26

---

## Dostepne interfejsy lokalne

- Kafka UI: http://localhost:8080
- JupyterLab: http://localhost:8888, token/haslo: `rta`
- Grafana: http://localhost:3000, login: `admin` / `admin`
- InfluxDB: http://localhost:8086, login: `admin` / `adminadmin`

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

**Cel systemu:** ocenic ryzyko kazdej transakcji w czasie < 200 ms i zwrocic
predykcje modelu ML zanim bank zatwierdzi przelew.

---

## 2. Architektura systemu

```
generator.py  -->  Kafka (topics: transactions, app_events)  -->  Spark Streaming  -->  Kafka (topic: alerts)  -->  alert_bridge.py  -->  InfluxDB  -->  Grafana
```

Szczegolowo:

```
+------------------------------------------------------------------+
|                        DATA GENERATOR                            |
|  profiles.py --> event_builder.py --> fraud_scenarios.py         |
|                       generator.py                               |
+------------------------------+-----------------------------------+
                               | JSON / kafka-python (50 tx/s)
               +---------------v--------------+
               |         Apache Kafka          |
               |  topic: transactions          |
               |  topic: app_events            |
               +------+------------------+-----+
                      |
         +------------v-----------------------------+
         |      Apache Spark Structured Streaming   |
         |  - parsowanie JSON                       |
         |  - feature engineering                   |
         |  - scoring modelem XGBoost               |
         |  - okna czasowe (5 min, 1 h)             |
         |  - publikacja alertow                    |
         +-------------------+----------------------+
                             |
               +-------------v--------------+
               |   Kafka topic: alerts       |
               |  { tx_id, fraud_probability,|
               |    predicted_fraud,         |
               |    fraud_type,              |
               |    sender_lat/lon/city }    |
               +-------------+--------------+
                             |
               +-------------v--------------+
               |      alert_bridge.py        |
               |   Kafka -> InfluxDB         |
               +-------------+--------------+
                             |
               +-------------v--------------+
               |         InfluxDB 2.x        |
               |   bucket: fraud_alerts      |
               +-------------+--------------+
                             |
               +-------------v--------------+
               |         Grafana            |
               |   dashboard real-time      |
               |   - Throughput (tx/min)    |
               |   - Fraud Rate (%)         |
               |   - Rozklad scenariuszy    |
               |   - Mapa GPS anomalii      |
               +----------------------------+
```

---

## 3. Status realizacji

| Krok | Komponent | Status |
|---|---|---|
| 1 | Silnik generowania danych (`data_generator/`) | ✅ **GOTOWE** |
| 2 | Infrastruktura Kafka (Docker Compose) | ✅ **GOTOWE** |
| 3 | Weryfikacja polaczenia generator -> Kafka | ✅ **GOTOWE** |
| 4 | Eksport datasetu offline (`data_generator/export_datasets.py`) | ✅ **GOTOWE** |
| 5 | Notebook treningowy ML (`fraud_model_training.ipynb`) | ✅ **GOTOWE** |
| 6 | Modele offline (`models/`) | ✅ **GOTOWE** |
| 7 | Spark Structured Streaming job | ✅ **GOTOWE** |
| 8 | Feature engineering online (okna czasowe) | ✅ **GOTOWE** |
| 9 | Scoring online / publikacja alertow | ✅ **GOTOWE** |
| 10 | Dashboard Grafana | ✅ **GOTOWE** |

---

## 4. Zrealizowane komponenty

### 4.1 `data_generator/` - silnik generowania danych

```
data_generator/
    profiles.py          # pula uzytkownikow: normal / mule / fraudster
    event_builder.py     # budowanie payloadow JSON dla obu tematow Kafka
    export_datasets.py   # eksport CSV/JSONL do treningu offline
    fraud_scenarios.py   # 5 wzorcow oszustw jako funkcje mutujace payload
    generator.py         # glowna petla + CLI + integracja z Kafka
    schemas.py           # wspolna definicja pol tematow Kafka
    verify_kafka.py      # konsument weryfikacyjny - czyta wiadomosci z Kafki
    requirements.txt
```

**Populacja uzytkownikow (profiles.py):**
- normal (90%) - realistyczne parametry, polskie/europejskie miasta z GPS
- mule (7%)    - mlode konta, duzo transakcji, posrednicy w layeringu
- fraudster (3%) - bardzo nowe konta, wysokie kwoty

**Schematy wiadomosci:**

Temat `transactions` (zawiera rowniez pola z app_event - embedowane przy generowaniu):
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
  "sender_avg_amount": 293.92,
  "pin_failures": 0,
  "device_changed": false,
  "is_offhours_login": false,
  "session_duration_sec": 119,
  "app_version": "3.7.3"
}
```

Temat `app_events` (publikowany rownoczesnie, uzywany do monitorowania):
```json
{
  "event_id": "uuid",
  "user_id": "uuid",
  "tx_id": "uuid",
  "timestamp": "2026-04-20T12:17:37+00:00",
  "pin_failures": 0,
  "device_changed": false,
  "new_device_id": "",
  "is_offhours_login": false,
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

### 4.2 `docker-compose.yml` - pelna infrastruktura

Uruchamia wszystkie serwisy jednym poleceniem:

| Kontener | Obraz | Port | Rola |
|---|---|---|---|
| `rta_kafka` | `apache/kafka:latest` | 9092 | Broker Kafka (KRaft) |
| `rta_kafka_ui` | `provectuslabs/kafka-ui` | 8080 | Interfejs webowy Kafki |
| `rta_jupyter` | `jupyterlab-project-jupyter:latest` | 8888 | JupyterLab (token: `rta`) |
| `rta_influxdb` | `influxdb:2.7` | 8086 | Baza szeregów czasowych |
| `rta_data_generator` | `python:3.11-slim` | - | Generator 50 tx/s, 10% fraudów |
| `rta_spark` | `jupyterlab-project-jupyter:latest` | - | Spark Streaming + scoring ML |
| `rta_alert_bridge` | `python:3.11-slim` | - | Kafka alerts → InfluxDB |
| `rta_grafana` | `grafana/grafana:latest` | 3000 | Dashboard real-time |

### 4.3 Dataset i modele offline

Repozytorium zawiera eksport danych syntetycznych do treningu batch:

```
datasets/
    fraud_events_100k.csv
    fraud_events_100k.jsonl
```

Dataset mozna odtworzyc poleceniem:

```bash
python data_generator/export_datasets.py --rows 100000 --fraud 0.10
```

Notebook `fraud_model_training.ipynb`:
- wczytuje pojedynczy dataset z cechami i etykieta (`label` / `is_fraud`),
- usuwa kolumny identyfikatorow i leakage (`label`, `is_fraud`, `fraud_type`),
- trenuje Logistic Regression, Random Forest i XGBoost,
- porownuje modele metrykami ROC AUC i F1.

Ostatni trening wskazal XGBoost jako najlepszy model:

| Model | ROC AUC | F1 |
|---|---:|---:|
| XGBoost | 0.9993 | 0.9345 |
| Logistic Regression | 0.9972 | 0.8862 |
| Random Forest | 0.9921 | 0.7907 |

Zapisane artefakty:

```
models/
    logistic_regression.joblib
    xgboost.joblib
```

### 4.4 `spark_job/fraud_detector.py` - real-time scoring pipeline

Trzy rownolegle strumienie Spark Structured Streaming:

**1. Fraud scoring (glowny pipeline):**
1. Czyta strumien z tematu Kafka `transactions`.
2. Parsuje JSON wedlug schematu z `data_generator/schemas.py`.
3. Liczy cechy online zsynchronizowane z notebookiem treningowym.
4. Wczytuje model `models/xgboost.joblib` i wywoluje `predict_proba`.
5. Publikuje alerty do tematu Kafka `alerts` z polami:
   `tx_id`, `fraud_probability`, `predicted_fraud`, `fraud_type`, `sender_lat/lon/city`.

**Cechy online (22 cechy, identyczny zbior co w treningu):**

| Cecha | Zrodlo | Opis |
|---|---|---|
| `hour`, `dayofweek`, `is_weekend` | `timestamp` | cechy temporalne |
| `amount_log1p` | `amount` | log(1 + amount), stabilizuje rozklad |
| `amount_to_sender_avg` | tx + profil | stosunek kwoty do sredniej nadawcy |
| `device_trusted`, `device_type` | `transactions` | flagi urzadzenia |
| `pin_failures`, `device_changed`, `is_offhours_login` | embedowane w tx | sygnaly z aplikacji mobilnej |
| `session_duration_sec`, `app_version` | embedowane w tx | metadane sesji |
| `sender_recipient_pair` | `sender_id->recipient_id` | cecha kategoryczna |

**2. Licznik transakcji (monitoring):**
- `tx_count_last_5min` — okno 5 min per `sender_id` (`build_counts_pipeline`)

**3. Unikalni odbiorcy (monitoring):**
- `unique_recipients_1h` — okno 1 h per `sender_id` (`build_recipients_pipeline`)

### 4.5 `alert_bridge.py` - most Kafka → InfluxDB

Konsumuje temat `alerts`, zapisuje do InfluxDB bucket `fraud_alerts`:
- tagi: `predicted_fraud`, `fraud_type`
- pola: `is_fraud` (0/1), `fraud_probability`, `tx_id`, `sender_lat`, `sender_lon`, `sender_city`

### 4.6 `grafana/` - dashboard real-time

4 panele aktualizowane co 5 sekund:

| Panel | Typ | Opis |
|---|---|---|
| Throughput | Time series | Liczba transakcji na minute |
| Fraud Rate | Time series | % transakcji oznaczonych jako fraud |
| Rozklad scenariuszy | Pie chart | Udzial kazdego typu oszustwa |
| Mapa GPS anomalii | Geomap | Lokalizacje podejrzanych transakcji na mapie Polski |

---

## 5. Uruchomienie

### Jedyne polecenie potrzebne do startu

```bash
docker compose up -d
```

Wszystkie serwisy startuja automatycznie w odpowiedniej kolejnosci.
Pierwsze uruchomienie pobiera obrazy i instaluje zaleznoci (~2-3 min).

### Sprawdzenie statusu

```bash
docker ps
# Wszystkie kontenery powinny byc "healthy" lub "Up"

docker logs rta_spark --follow
# Oczekiwany output:
# [SCORER] Model loaded OK
# [SCORER] batch=1 rows=~600 fraud=~60 (10.0%)
```

### Zatrzymanie i restart z czystym stanem

```bash
docker compose down -v   # usuwa tez wolumeny (Kafka, InfluxDB)
docker compose up -d
```

### Interfejsy webowe

| Serwis | URL | Dane logowania |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| JupyterLab | http://localhost:8888 | token: `rta` |
| Kafka UI | http://localhost:8080 | - |
| InfluxDB | http://localhost:8086 | admin / adminadmin |

### Parametry generatora (opcjonalne)

Domyslnie: 50 tx/s, 10% fraudow. Aby zmienic, edytuj `docker-compose.yml`:

```yaml
command: >
  bash -c "pip install kafka-python --quiet && 
           python generator.py --rate 100 --fraud 0.15 --bootstrap kafka:29092"
```

### Trenowanie modelu od nowa

Otworz JupyterLab (http://localhost:8888, token `rta`) i uruchom notebook:
```
fraud_model_training.ipynb
```

Model zapisze sie do `models/xgboost.joblib`. Spark zaladuje go przy nastepnym restarcie.
