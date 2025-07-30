"""
Client HTTP centralisé avec connection pooling pour optimiser les performances
"""

import aiohttp
import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from config.settings import settings


class HTTPClientManager:
    """Gestionnaire centralisé des clients HTTP avec pooling de connexions"""
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        
        # Configuration optimisée pour performance
        self._connector_config = {
            'limit': 100,  # Limite totale de connexions
            'limit_per_host': 30,  # Limite par host
            'ttl_dns_cache': 10 * 60,  # Cache DNS 10 minutes
            'use_dns_cache': True,
            'keepalive_timeout': 30,  # Keep-alive 30s
            'enable_cleanup_closed': True
        }
        
        self._timeout_config = {
            'total': 30,  # Timeout total par défaut
            'connect': 10,  # Timeout connexion
            'sock_read': 20  # Timeout lecture
        }
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Retourne la session HTTP réutilisable (singleton)"""
        if self._session is None or self._session.closed:
            async with self._lock:
                if self._session is None or self._session.closed:
                    await self._create_session()
        
        return self._session
    
    async def _create_session(self):
        """Crée une nouvelle session HTTP optimisée"""
        connector = aiohttp.TCPConnector(**self._connector_config)
        timeout = aiohttp.ClientTimeout(**self._timeout_config)
        
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'HotelScraper/1.0 (Professional Data Extraction)'
            },
            # Cookies automatiques pour sessions
            cookie_jar=aiohttp.CookieJar()
        )
        
        print(f"🔗 Session HTTP créée avec connection pooling")
        print(f"   📊 Limites: {self._connector_config['limit']} total, {self._connector_config['limit_per_host']}/host")
    
    async def close(self):
        """Ferme proprement la session HTTP"""
        if self._session and not self._session.closed:
            await self._session.close()
            print("🔒 Session HTTP fermée")
    
    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """GET request avec session poolée"""
        session = await self.get_session()
        return await session.get(url, **kwargs)
    
    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """POST request avec session poolée"""
        session = await self.get_session()
        return await session.post(url, **kwargs)
    
    async def request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """Request générique avec session poolée"""
        session = await self.get_session()
        return await session.request(method, url, **kwargs)
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de connexion"""
        if not self._session or self._session.closed:
            return {'status': 'no_session'}
        
        connector = self._session.connector
        if hasattr(connector, '_conns'):
            return {
                'status': 'active',
                'total_connections': len(connector._conns),
                'acquired_connections': sum(len(conns) for conns in connector._conns.values()),
                'connector_config': self._connector_config
            }
        
        return {'status': 'active', 'details': 'unavailable'}


# Instance globale pour réutilisation
_http_manager: Optional[HTTPClientManager] = None

async def get_http_client() -> HTTPClientManager:
    """Retourne l'instance globale du client HTTP"""
    global _http_manager
    if _http_manager is None:
        _http_manager = HTTPClientManager()
    return _http_manager

@asynccontextmanager
async def http_session():
    """Context manager pour obtenir une session HTTP poolée"""
    client = await get_http_client()
    session = await client.get_session()
    try:
        yield session
    finally:
        # La session est réutilisée, pas fermée
        pass

async def close_http_client():
    """Ferme le client HTTP global"""
    global _http_manager
    if _http_manager:
        await _http_manager.close()
        _http_manager = None


# Fonctions utilitaires pour remplacer aiohttp direct
async def http_get(url: str, **kwargs):
    """GET request optimisé avec pooling"""
    client = await get_http_client()
    return await client.get(url, **kwargs)

async def http_post(url: str, **kwargs):
    """POST request optimisé avec pooling"""
    client = await get_http_client()
    return await client.post(url, **kwargs)

async def http_request(method: str, url: str, **kwargs):
    """Request générique optimisé avec pooling"""
    client = await get_http_client()
    return await client.request(method, url, **kwargs)