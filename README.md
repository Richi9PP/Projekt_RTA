# Real-Time Fraud Detection for P2P Payments

> Projekt na przedmiot **Analiza Danych w Czasie Rzeczywistym** - SGH 2025/26

---

## Dostepne interfejsy lokalne

- Kafka UI: http://localhost:8080
- JupyterLab: http://localhost:8888, token/haslo: `rta`

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
               |  { tx_id, fraud_probability,|
               |    predicted_fraud,         |
               |    fraud_type }             |
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
| 4 | Eksport datasetu offline (`data_generator/export_datasets.py`) | **GOTOWE** |
| 5 | Notebook treningowy ML (`fraud_model_training.ipynb`) | **GOTOWE** |
| 6 | Modele offline (`models/`) | **GOTOWE** |
| 7 | Spark Structured Streaming job | TODO |
| 8 | Feature engineering online (okna czasowe) | TODO |
| 9 | Scoring online / publikacja alertow | TODO |
| 10 | Dashboard Grafana | TODO |

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

Uruchamia trzy kontenery:
- **rta_kafka** (`apache/kafka:latest`) - broker KRaft (bez Zookeepera), port 9092
- **rta_kafka_ui** (`provectuslabs/kafka-ui`) - interfejs webowy, port 8080
- **rta_jupyter** (`jupyterlab-project-jupyter:latest`) - JupyterLab, port 8888, token `rta`

Tematy tworzone sa automatycznie przy pierwszej publikacji (`AUTO_CREATE_TOPICS_ENABLE=true`).

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

---

## 5. Uruchomienie

### Krok 1 - Instalacja zaleznosci lokalnych

```bash
pip install -r requirements.txt
```

### Krok 2 - Start Kafki, Kafka UI i JupyterLab

```bash
docker compose up -d
# Poczekaj az kafka przejdzie w stan healthy (~30 s)
docker ps  # kolumna STATUS powinna pokazac "(healthy)"
```

### Krok 3 - Uruchomienie generatora

```bash
# Wyslij dane do Kafki (10 tx/s, 10% fraudow)
python data_generator/generator.py --rate 10 --fraud 0.10

# Test bez Kafki (drukuje JSON na stdout)
python data_generator/generator.py --dry-run --rate 5
```

### Krok 4 - Weryfikacja odczytu z Kafki

```bash
python data_generator/verify_kafka.py --n 10
```

### Krok 5 - Eksport datasetu offline

```bash
python data_generator/export_datasets.py --rows 100000 --fraud 0.10
```

### Interfejsy webowe

- Kafka UI: http://localhost:8080
- JupyterLab: http://localhost:8888, token/haslo `rta`

---

## 6. Nastepne kroki

### Krok 7 - Apache Spark Structured Streaming

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

### Krok 8 - Scoring online

Wczytanie zapisanego modelu z `models/xgboost.joblib` i publikacja predykcji
ML bez recznie ustawianych progow decyzyjnych.

### Krok 9 - Dashboard Grafana

Grafana + plugin Kafka -> wizualizacja real-time:
- throughput (tx/s)
- fraud rate (%)
- rozklad scenariuszy oszustw
- mapa GPS anomalii
