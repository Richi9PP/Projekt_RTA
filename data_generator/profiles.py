"""
Static synthetic profiles: users, devices, locations.
Generated once at startup and reused across events to ensure
referential consistency (same user → consistent home city, device pool, etc.).
"""

import random
import uuid
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Geographic anchors – realistic Polish / EU cities with lat/lon bounding boxes
# ---------------------------------------------------------------------------
CITY_BOXES = {
    "Warsaw":    {"lat": (52.10, 52.35), "lon": (20.85, 21.20), "country": "PL"},
    "Krakow":    {"lat": (49.97, 50.12), "lon": (19.85, 20.10), "country": "PL"},
    "Gdansk":    {"lat": (54.30, 54.45), "lon": (18.50, 18.80), "country": "PL"},
    "Wroclaw":   {"lat": (51.05, 51.18), "lon": (16.90, 17.10), "country": "PL"},
    "Poznan":    {"lat": (52.35, 52.50), "lon": (16.85, 17.05), "country": "PL"},
    "Berlin":    {"lat": (52.40, 52.60), "lon": (13.25, 13.55), "country": "DE"},
    "London":    {"lat": (51.40, 51.60), "lon": (-0.25,  0.10), "country": "GB"},
    "Moscow":    {"lat": (55.60, 55.85), "lon": (37.40, 37.80), "country": "RU"},
    "Lagos":     {"lat": (6.40,  6.65), "lon": (3.25,   3.55), "country": "NG"},
    "Bucharest": {"lat": (44.35, 44.55), "lon": (26.00, 26.25), "country": "RO"},
}

HIGH_RISK_CITIES = {"Moscow", "Lagos"}

CURRENCIES = ["PLN", "EUR", "USD", "GBP"]

DEVICE_TYPES = ["android", "ios", "web"]


def _realistic_public_ip() -> str:
    """Generate a routable public IP, excluding RFC1918, loopback, and link-local ranges."""
    while True:
        a = random.randint(1, 223)
        b = random.randint(0, 255)
        c = random.randint(0, 255)
        d = random.randint(1, 254)
        if a == 10:
            continue
        if a == 127:
            continue
        if a == 172 and 16 <= b <= 31:
            continue
        if a == 192 and b == 168:
            continue
        if a == 169 and b == 254:
            continue
        return f"{a}.{b}.{c}.{d}"


def _coords_for_city(city: str) -> tuple[float, float]:
    box = CITY_BOXES[city]
    lat = round(random.uniform(*box["lat"]), 6)
    lon = round(random.uniform(*box["lon"]), 6)
    return lat, lon


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Device:
    device_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    device_type: str = field(default_factory=lambda: random.choice(DEVICE_TYPES))
    os_version: str = ""
    trusted: bool = True

    def __post_init__(self):
        versions = {"android": "14.0", "ios": "17.2", "web": "n/a"}
        self.os_version = versions[self.device_type]


@dataclass
class UserProfile:
    user_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    home_city: str = field(default_factory=lambda: random.choice(
        ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]))
    preferred_currency: str = field(default_factory=lambda: random.choice(CURRENCIES))
    avg_tx_amount: float = field(default_factory=lambda: round(random.uniform(50, 500), 2))
    monthly_tx_count: int = field(default_factory=lambda: random.randint(5, 60))
    account_age_days: int = field(default_factory=lambda: random.randint(30, 3650))
    devices: list[Device] = field(default_factory=list)
    risk_label: str = "normal"

    def __post_init__(self):
        n_devices = random.randint(1, 3)
        self.devices = [Device() for _ in range(n_devices)]
        if self.risk_label == "mule":
            self.account_age_days = random.randint(1, 90)
            self.monthly_tx_count = random.randint(30, 200)
        elif self.risk_label == "fraudster":
            self.account_age_days = random.randint(1, 30)
            self.avg_tx_amount = round(random.uniform(500, 5000), 2)

    @property
    def primary_device(self) -> Device:
        return self.devices[0]

    def home_coords(self) -> tuple[float, float]:
        return _coords_for_city(self.home_city)


def build_user_pool(n_normal: int = 900,
                    n_mules: int = 70,
                    n_fraudsters: int = 30) -> list[UserProfile]:
    pool = [UserProfile() for _ in range(n_normal)]
    pool += [UserProfile(risk_label="mule") for _ in range(n_mules)]
    pool += [UserProfile(risk_label="fraudster") for _ in range(n_fraudsters)]
    random.shuffle(pool)
    return pool
