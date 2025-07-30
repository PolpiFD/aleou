"""
Tests unitaires pour le système de cache Google Maps
"""

import pytest
import pytest_asyncio
import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from cache.gmaps_cache import GoogleMapsCache


class TestGoogleMapsCache:
    """Tests pour le cache Google Maps"""
    
    @pytest_asyncio.fixture
    async def cache(self):
        """Fixture avec cache temporaire"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = Path(temp_dir) / "test_cache.json"
            cache_instance = GoogleMapsCache(cache_file=str(cache_file), ttl=3600)
            await cache_instance.initialize()
            yield cache_instance
    
    @pytest.fixture
    def sample_gmaps_data(self):
        return {
            'success': True,
            'name': 'Test Hotel',
            'address': 'Test Address',
            'phone': '+32 2 123 4567',
            'website': 'https://test-hotel.com',
            'rating': '4.5',
            'place_id': 'test_place_123'
        }
    
    def test_generate_cache_key(self, cache):
        """Test génération des clés de cache"""
        key1 = cache._generate_cache_key("Hotel Test", "123 Rue Test")
        key2 = cache._generate_cache_key("hotel test", "123 rue test")  # Même clé normalisée
        key3 = cache._generate_cache_key("Different Hotel", "123 Rue Test")
        
        assert key1 == key2  # Normalisation fonctionne
        assert key1 != key3  # Clés différentes pour données différentes
        assert len(key1) == 32  # MD5 hash
    
    @pytest.mark.asyncio
    async def test_set_and_get(self, cache, sample_gmaps_data):
        """Test sauvegarde et récupération de base"""
        hotel_name = "Test Hotel"
        hotel_address = "123 Test Street"
        
        # Sauvegarde
        await cache.set(hotel_name, hotel_address, sample_gmaps_data)
        
        # Récupération
        retrieved_data = await cache.get(hotel_name, hotel_address)
        
        assert retrieved_data is not None
        assert retrieved_data['success'] == True
        assert retrieved_data['name'] == 'Test Hotel'
        assert retrieved_data['phone'] == '+32 2 123 4567'
    
    @pytest.mark.asyncio  
    async def test_get_nonexistent(self, cache):
        """Test récupération d'une entrée inexistante"""
        result = await cache.get("Nonexistent Hotel", "Nowhere Address")
        assert result is None
        
        # Vérifier stats
        stats = cache.get_stats()
        assert stats['misses'] == 1
        assert stats['hits'] == 0
    
    @pytest.mark.asyncio
    async def test_expiration(self, cache, sample_gmaps_data):
        """Test expiration des entrées"""
        # Cache avec TTL très court
        cache.ttl = 0.1  # 0.1 seconde
        
        await cache.set("Test Hotel", "Test Address", sample_gmaps_data)
        
        # Récupération immédiate (devrait marcher)
        result1 = await cache.get("Test Hotel", "Test Address")
        assert result1 is not None
        
        # Attendre expiration
        await asyncio.sleep(0.2)
        
        # Récupération après expiration (devrait retourner None)
        result2 = await cache.get("Test Hotel", "Test Address")
        assert result2 is None
        
        # Vérifier stats d'expiration
        stats = cache.get_stats()
        assert stats['expired'] == 1
    
    @pytest.mark.asyncio
    async def test_batch_operations(self, cache, sample_gmaps_data):
        """Test opérations batch"""
        hotels_data = [
            {'name': 'Hotel A', 'address': 'Address A'},
            {'name': 'Hotel B', 'address': 'Address B'},
            {'name': 'Hotel C', 'address': 'Address C'}
        ]
        
        results_data = [
            {**sample_gmaps_data, 'name': 'Hotel A'},
            {**sample_gmaps_data, 'name': 'Hotel B'},
            {**sample_gmaps_data, 'name': 'Hotel C'}
        ]
        
        # Sauvegarde batch
        await cache.batch_set(hotels_data, results_data)
        
        # Récupération batch
        batch_results = await cache.batch_get(hotels_data)
        
        # Vérifications
        assert len(batch_results) == 3
        
        for hotel_data in hotels_data:
            cache_key = cache._generate_cache_key(hotel_data['name'], hotel_data['address'])
            assert cache_key in batch_results
            assert batch_results[cache_key] is not None
            assert batch_results[cache_key]['name'] == hotel_data['name']
    
    @pytest.mark.asyncio
    async def test_persistence(self, sample_gmaps_data):
        """Test persistance sur disque"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = Path(temp_dir) / "persistence_test.json"
            
            # Première instance - sauvegarder des données
            cache1 = GoogleMapsCache(cache_file=str(cache_file), ttl=3600)
            await cache1.initialize()
            await cache1.set("Persistent Hotel", "Test Address", sample_gmaps_data)
            await cache1._save_to_disk()
            
            # Vérifier que le fichier existe
            assert cache_file.exists()
            
            # Deuxième instance - charger les données
            cache2 = GoogleMapsCache(cache_file=str(cache_file), ttl=3600)
            await cache2.initialize()
            
            # Récupérer les données
            retrieved = await cache2.get("Persistent Hotel", "Test Address")
            
            assert retrieved is not None
            assert retrieved['name'] == 'Test Hotel'
            assert retrieved['phone'] == '+32 2 123 4567'
    
    @pytest.mark.asyncio
    async def test_cleanup_expired(self, cache, sample_gmaps_data):
        """Test nettoyage des entrées expirées"""
        # Ajouter plusieurs entrées avec différents TTL
        cache.ttl = 0.1  # TTL très court
        
        await cache.set("Hotel 1", "Address 1", sample_gmaps_data)
        await cache.set("Hotel 2", "Address 2", sample_gmaps_data)
        await cache.set("Hotel 3", "Address 3", sample_gmaps_data)
        
        # Attendre expiration
        await asyncio.sleep(0.2)
        
        # Ajouter une entrée récente
        cache.ttl = 3600  # TTL normal
        await cache.set("Hotel 4", "Address 4", sample_gmaps_data)
        
        # Nettoyer les expirées
        deleted_count = await cache.cleanup_expired()
        
        # Vérifications
        assert deleted_count == 3  # 3 entrées expirées supprimées
        
        # L'entrée récente devrait toujours être là
        recent_data = await cache.get("Hotel 4", "Address 4")
        assert recent_data is not None
    
    @pytest.mark.asyncio
    async def test_clear_cache(self, cache, sample_gmaps_data):
        """Test vidage complet du cache"""
        # Ajouter des données
        await cache.set("Hotel 1", "Address 1", sample_gmaps_data)
        await cache.set("Hotel 2", "Address 2", sample_gmaps_data)
        
        # Vérifier présence
        assert await cache.get("Hotel 1", "Address 1") is not None
        
        # Vider le cache
        await cache.clear()
        
        # Vérifier que tout est vidé
        assert await cache.get("Hotel 1", "Address 1") is None
        assert await cache.get("Hotel 2", "Address 2") is None
        
        stats = cache.get_stats()
        assert stats['cache_size'] == 0
    
    def test_cache_stats(self, cache):
        """Test génération des statistiques"""
        stats = cache.get_stats()
        
        required_keys = [
            'cache_size', 'hit_rate', 'hits', 'misses', 
            'expired', 'saves', 'loads', 'cache_file', 'ttl_hours'
        ]
        
        for key in required_keys:
            assert key in stats
        
        assert stats['hit_rate'].endswith('%')
        assert stats['ttl_hours'] == 1.0  # 3600s = 1h
        assert isinstance(stats['cache_size'], int)
    
    @pytest.mark.asyncio
    async def test_context_manager(self, sample_gmaps_data):
        """Test utilisation comme context manager"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = Path(temp_dir) / "context_test.json"
            
            async with GoogleMapsCache(cache_file=str(cache_file)) as cache:
                await cache.set("Context Hotel", "Context Address", sample_gmaps_data)
                
                retrieved = await cache.get("Context Hotel", "Context Address")
                assert retrieved is not None
            
            # Vérifier que le fichier a été sauvegardé
            assert cache_file.exists()
    
    @pytest.mark.asyncio 
    async def test_concurrent_access(self, cache, sample_gmaps_data):
        """Test accès concurrent au cache"""
        
        async def set_data(cache, index):
            await cache.set(f"Hotel {index}", f"Address {index}", {
                **sample_gmaps_data, 
                'name': f'Hotel {index}'
            })
        
        async def get_data(cache, index):
            return await cache.get(f"Hotel {index}", f"Address {index}")
        
        # Lancer plusieurs opérations en concurrent
        set_tasks = [set_data(cache, i) for i in range(10)]
        await asyncio.gather(*set_tasks)
        
        get_tasks = [get_data(cache, i) for i in range(10)]
        results = await asyncio.gather(*get_tasks)
        
        # Vérifier que toutes les données sont présentes
        assert len(results) == 10
        assert all(r is not None for r in results)
        assert all(r['name'] == f'Hotel {i}' for i, r in enumerate(results))
    
    @pytest.mark.asyncio
    async def test_cache_performance_measurement(self, cache, sample_gmaps_data):
        """Test mesure des performances du cache"""
        
        # Simuler un scenario réaliste
        hotels = [(f"Hotel {i}", f"Address {i}") for i in range(50)]
        
        # Premier passage - misses (cold cache)
        for name, address in hotels[:25]:
            result = await cache.get(name, address)
            assert result is None  # Cache miss
            
            # Simuler récupération API et mise en cache
            await cache.set(name, address, {**sample_gmaps_data, 'name': name})
        
        # Deuxième passage - hits (warm cache)
        for name, address in hotels[:25]:
            result = await cache.get(name, address)
            assert result is not None  # Cache hit
        
        # Stats finales
        stats = cache.get_stats()
        
        # 25 misses + 25 hits = 50 total, 50% hit rate
        assert stats['hits'] == 25
        assert stats['misses'] == 25
        assert stats['hit_rate'] == "50.0%"
        assert stats['cache_size'] == 25


@pytest.mark.asyncio
async def test_global_cache_instance():
    """Test de l'instance globale du cache"""
    from cache.gmaps_cache import get_global_cache, cache_cleanup
    
    # Obtenir l'instance globale
    cache1 = await get_global_cache()
    cache2 = await get_global_cache()
    
    # Vérifier que c'est la même instance (singleton)
    assert cache1 is cache2
    
    # Test du cleanup global
    await cache_cleanup()  # Ne devrait pas lever d'exception