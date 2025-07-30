import json
import hashlib
import asyncio
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import aiofiles


class GoogleMapsCache:
    """Simple async cache for Google Maps data with TTL."""

    def __init__(self, cache_file: str = "cache/gmaps_cache.json", ttl: int = 3600):
        self.cache_file = Path(cache_file)
        self.ttl = ttl
        self._data: Dict[str, Dict[str, Any]] = {}
        self.hits = 0
        self.misses = 0
        self.expired = 0
        self.saves = 0
        self.loads = 0
        self._lock = asyncio.Lock()

    async def initialize(self):
        await self._load_from_disk()
        return self

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._save_to_disk()

    def _generate_cache_key(self, hotel_name: str, hotel_address: str) -> str:
        key = f"{hotel_name.lower().strip()}|{hotel_address.lower().strip()}"
        return hashlib.md5(key.encode("utf-8")).hexdigest()

    async def get(self, hotel_name: str, hotel_address: str) -> Optional[Dict[str, Any]]:
        key = self._generate_cache_key(hotel_name, hotel_address)
        async with self._lock:
            entry = self._data.get(key)
            if not entry:
                self.misses += 1
                return None
            ttl = entry.get("ttl", self.ttl)
            if time.time() - entry["timestamp"] > ttl:
                self.expired += 1
                self.misses += 1
                del self._data[key]
                return None
            self.hits += 1
            return entry["value"]

    async def set(self, hotel_name: str, hotel_address: str, data: Dict[str, Any]):
        key = self._generate_cache_key(hotel_name, hotel_address)
        async with self._lock:
            self._data[key] = {"timestamp": time.time(), "value": data, "ttl": self.ttl}

    async def batch_get(self, hotels: List[Dict[str, str]]) -> Dict[str, Optional[Dict[str, Any]]]:
        results: Dict[str, Optional[Dict[str, Any]]] = {}
        for h in hotels:
            value = await self.get(h["name"], h["address"])
            results[self._generate_cache_key(h["name"], h["address"])] = value
        return results

    async def batch_set(self, hotels: List[Dict[str, str]], results: List[Dict[str, Any]]):
        for h, r in zip(hotels, results):
            await self.set(h["name"], h["address"], r)

    async def cleanup_expired(self) -> int:
        now = time.time()
        async with self._lock:
            keys = [k for k, v in self._data.items() if now - v["timestamp"] > v.get("ttl", self.ttl)]
            for k in keys:
                del self._data[k]
            self.expired += len(keys)
            return len(keys)

    async def clear(self):
        async with self._lock:
            self._data.clear()
            self.hits = 0
            self.misses = 0
            self.expired = 0

    def get_stats(self) -> Dict[str, Any]:
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total else 0
        return {
            "cache_size": len(self._data),
            "hit_rate": f"{hit_rate:.1f}%",
            "hits": self.hits,
            "misses": self.misses,
            "expired": self.expired,
            "saves": self.saves,
            "loads": self.loads,
            "cache_file": str(self.cache_file),
            "ttl_hours": self.ttl / 3600,
        }

    async def _save_to_disk(self):
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(self.cache_file, "w") as f:
            await f.write(json.dumps(self._data))
        self.saves += 1

    async def _load_from_disk(self):
        if self.cache_file.exists():
            async with aiofiles.open(self.cache_file, "r") as f:
                try:
                    data = json.loads(await f.read() or "{}")
                    self._data = data
                except json.JSONDecodeError:
                    self._data = {}
            self.loads += 1


_global_cache: Optional[GoogleMapsCache] = None
_cache_lock = asyncio.Lock()


async def get_global_cache() -> GoogleMapsCache:
    global _global_cache
    async with _cache_lock:
        if _global_cache is None:
            from config.settings import settings
            _global_cache = GoogleMapsCache(
                cache_file=settings.cache.cache_file,
                ttl=settings.cache.cache_ttl,
            )
            await _global_cache.initialize()
        return _global_cache


async def cache_cleanup():
    cache = await get_global_cache()
    await cache.cleanup_expired()
    await cache._save_to_disk()
