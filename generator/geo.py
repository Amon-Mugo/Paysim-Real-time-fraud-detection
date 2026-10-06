#used to determine the geogrphyical location of users based on the accountid and use 
#sha256 used for cryptographic locationn

from __future__ import annotations
from dataclasses import dataclass
import math
import hashlib

EARTH_RADIUS_KM = 6371.0088  #earth radius
JITTER_DEGREES = 0.05   #maximusm shift
COORDINATE_PRECISION = 5  #ROUND THE coordinates to 5 decimal placeses

@dataclass(frozen= True)
class Location:
    longitude: float
    latitude: float

@dataclass(frozen=True)
class City:
    name: str
    centre: Location
    weight: int 

CITIES: tuple[City, ...] = (
    City("Nairobi", Location(-1.2921, 36.8219), 40),
    City("Mombasa", Location(-4.0435, 39.6682), 15),
    City("Kisumu", Location(-0.0917, 34.7680), 8),
    City("Nakuru", Location(-0.3031, 36.0800), 8),
    City("Eldoret", Location(0.5143, 35.2698), 7),
    City("Thika", Location(-1.0333, 37.0693), 5),
    City("Malindi", Location(-3.2192, 40.1169), 3),
    City("Kitale", Location(1.0157, 35.0062), 3),
    City("Garissa", Location(-0.4532, 39.6461), 3),
    City("Nyeri", Location(-0.4201, 36.9476), 3),
    City("Machakos", Location(-1.5177, 37.2634), 3),
    City("Meru", Location(0.0467, 37.6490), 2),
)

_TOTAL_WEIGHT = sum(city.weight for city in CITIES)

def home_location (account_id:str)-> Location:
    digest= hashlib.sha256(account_id.encode("utf-8")).digest()

    city = _pick_city(int.from_bytes(digest[0:8],"big")% _TOTAL_WEIGHT)

    latitude_offset = _offset(digest[8:12])
    longitude_offset = _offset(digest[12:16])

    return Location(
        latitude=round(city.centre.latitude + latitude_offset, COORDINATE_PRECISION),
        longitude=round(city.centre.longitude + longitude_offset, COORDINATE_PRECISION),
    )

def haversine_km(first: Location, second: Location) -> float:
    """Return the great-circle distance between two locations in kilometres."""
    lat1, lon1 = math.radians(first.latitude), math.radians(first.longitude)
    lat2, lon2 = math.radians(second.latitude), math.radians(second.longitude)

    half_chord = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(half_chord))


def _pick_city(slot: int) -> City:
    cumulative = 0
    for city in CITIES:
        cumulative += city.weight
        if slot < cumulative:
            return city
    raise AssertionError("slot exceeded total city weight")


def _offset(raw: bytes) -> float:

    fraction = int.from_bytes(raw, "big") / 2**32
    return (fraction * 2 - 1) * JITTER_DEGREES