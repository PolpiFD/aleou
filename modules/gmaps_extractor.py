"""
Module d'extraction Google Maps
Utilise la nouvelle API Places (New) de Google pour extraire les informations hôtelières
"""

import os
import asyncio
import aiohttp
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass

# Charger automatiquement le fichier .env


from .rate_limiter import rate_limit_manager, RateLimitConfig
from cache.gmaps_cache import get_global_cache
from config.settings import settings


@dataclass
class GoogleMapsConfig:
    """Configuration pour l'API Google Maps"""
    api_key: str
    base_url: str = "https://places.googleapis.com/v1"
    timeout: int = 30
    max_retries: int = 3
    
    @classmethod
    def from_env(cls):
        """Crée la configuration depuis les variables d'environnement"""
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if not api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY non trouvée dans les variables d'environnement")
        
        return cls(api_key=api_key)


class GoogleMapsExtractor:
    """Extracteur d'informations Google Maps pour hôtels"""
    
    def __init__(self, config: GoogleMapsConfig = None):
        self.config = config or GoogleMapsConfig.from_env()
        
        # Configuration du rate limiter Google Maps
        rate_config = RateLimitConfig(
            requests_per_minute=int(os.getenv('GOOGLE_MAPS_QUOTA_PER_MINUTE', 60)),
            requests_per_second=5,
            burst_requests=3
        )
        self.rate_limiter = rate_limit_manager.get_limiter('google_maps', rate_config)
        
        # Headers pour l'API Places (New)
        self.headers = {
            'X-Goog-Api-Key': self.config.api_key,
            'X-Goog-FieldMask': self._get_field_mask(),
            'Content-Type': 'application/json'
        }
    
    def _get_field_mask(self) -> str:
        """Définit les champs à récupérer de l'API Places
        
        Returns:
            str: Masque de champs pour l'API
        """
        fields = [
            'places.id',
            'places.name',  # 🔧 RETOUR: name (avec extraction intelligente)
            'places.formattedAddress',
            'places.location',
            'places.rating',
            'places.userRatingCount',
            'places.websiteUri',
            'places.nationalPhoneNumber',
            'places.businessStatus',
            'places.primaryType',
            'places.types',
            'places.regularOpeningHours',
            'places.photos',
            'places.googleMapsUri'
        ]
        return ','.join(fields)
    
    async def extract_hotel_info(self, hotel_name: str, hotel_address: str) -> Dict[str, Any]:
        """Extrait les informations Google Maps pour un hôtel
        
        Args:
            hotel_name (str): Nom de l'hôtel
            hotel_address (str): Adresse de l'hôtel
            
        Returns:
            Dict[str, Any]: Informations extraites de Google Maps
        """
        
        search_queries = self._build_search_queries(hotel_name, hotel_address)
        
        for query in search_queries:
            try:
                result = await self._search_place(query)
                
                if result and self._is_valid_hotel_result(result, hotel_name):
                    # 🔧 TEMPORAIRE: utiliser directement les données de recherche
                    return self._format_hotel_data(result, hotel_name, hotel_address)
                    
            except Exception as e:
                print(f"⚠️ Erreur recherche '{query}': {e}")
                continue
        
        # Aucun résultat trouvé
        return self._create_empty_result(hotel_name, hotel_address, "Aucun résultat trouvé")
    
    def _build_search_queries(self, hotel_name: str, hotel_address: str) -> List[str]:
        """Construit plusieurs requêtes de recherche pour maximiser les chances de succès
        
        Args:
            hotel_name (str): Nom de l'hôtel
            hotel_address (str): Adresse de l'hôtel
            
        Returns:
            List[str]: Liste des requêtes à tester
        """
        
        queries = []
        
        # Nettoyage du nom (retirer "Hôtel", "Hotel", etc.)
        clean_name = hotel_name.replace("Hôtel", "").replace("Hotel", "").strip()
        
        # Extraction de la ville depuis l'adresse
        city = self._extract_city_from_address(hotel_address)
        
        # Construire différentes variations
        queries.append(f"{hotel_name} {city}")  # Nom complet + ville
        queries.append(f"{clean_name} hotel {city}")  # Nom nettoyé + hotel + ville
        queries.append(f"{hotel_name}")  # Nom complet seul
        queries.append(f"{clean_name} {hotel_address}")  # Nom + adresse complète
        
        return queries
    
    def _extract_city_from_address(self, address: str) -> str:
        """Extrait la ville depuis une adresse
        
        Args:
            address (str): Adresse complète
            
        Returns:
            str: Nom de la ville
        """
        # Vérifier si address est valide (pas NaN, None, ou vide)
        if not address or str(address).lower() in ['nan', 'none', '']:
            return ""
        
        # Convertir en string pour être sûr
        address_str = str(address).strip()
        if not address_str:
            return ""
        
        # Logique simple d'extraction de ville
        parts = address_str.split()
        
        # Chercher des indices de ville (codes postaux, mots-clés)
        for i, part in enumerate(parts):
            if part.isdigit() and len(part) in [4, 5]:  # Code postal
                if i + 1 < len(parts):
                    return parts[i + 1]
        
        # Fallback: prendre les derniers mots
        return ' '.join(parts[-2:]) if len(parts) >= 2 else parts[-1] if parts else ""
    
    async def _search_place(self, query: str) -> Optional[Dict[str, Any]]:
        """Effectue une recherche de lieu via l'API Places
        
        Args:
            query (str): Requête de recherche
            
        Returns:
            Optional[Dict[str, Any]]: Premier résultat trouvé ou None
        """
        
        await self.rate_limiter.acquire()
        
        search_data = {
            "textQuery": query,
            "maxResultCount": 5,
            "locationBias": {
                "circle": {
                    "center": {"latitude": 50.8505, "longitude": 4.3488},  # Brussels par défaut
                    "radius": 50000  # 50km
                }
            }
        }
        
        # Debug minimal pour Google Maps
        print(f"🔍 Recherche Google Maps: '{query}'")
        
        # 🔧 CORRECTION: Session aiohttp dédiée pour éviter "Event loop is closed"
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    f"{self.config.base_url}/places:searchText",
                    headers=self.headers,
                    json=search_data
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        places = data.get('places', [])
                        if places:
                            print(f"✅ Trouvé sur Google Maps")
                        return places[0] if places else None
                    
                    elif response.status == 403:
                        # Debug spécifique pour l'erreur 403
                        response_text = await response.text()
                        print(f"❌ Erreur 403 - Détails: {response_text}")
                        
                        # Analyser la réponse d'erreur
                        if "API key not valid" in response_text:
                            raise Exception("❌ Clé API invalide - Vérifiez votre GOOGLE_MAPS_API_KEY")
                        elif "API has not been used" in response_text:
                            raise Exception("❌ API Places pas activée - Activez-la dans Google Cloud Console")
                        elif "This IP" in response_text:
                            raise Exception("❌ IP non autorisée - Configurez les restrictions IP")
                        elif "quota" in response_text.lower():
                            raise Exception("❌ Quota dépassé - Vérifiez vos limites API")
                        else:
                            raise Exception(f"❌ Erreur 403: {response_text}")
                    
                    elif response.status == 429:
                        await self.rate_limiter.handle_error(429)
                        raise Exception("Rate limit dépassé")
                    
                    else:
                        response_text = await response.text()
                        print(f"❌ Erreur {response.status} - Détails: {response_text}")
                        await self.rate_limiter.handle_error(response.status)
                        raise Exception(f"Erreur API: {response.status}")
            
            except asyncio.TimeoutError:
                raise Exception("Timeout de la requête")
    
    async def _get_place_details(self, place_id: str) -> Dict[str, Any]:
        """Récupère les détails d'un lieu via son ID
        
        Args:
            place_id (str): ID du lieu Google Maps
            
        Returns:
            Dict[str, Any]: Détails complets du lieu
        """
        
        await self.rate_limiter.acquire()
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.timeout)) as session:
            try:
                async with session.get(
                    f"{self.config.base_url}/places/{place_id}",
                    headers=self.headers
                ) as response:
                    
                    if response.status == 200:
                        return await response.json()
                    
                    elif response.status == 429:
                        await self.rate_limiter.handle_error(429)
                        raise Exception("Rate limit dépassé")
                    
                    else:
                        await self.rate_limiter.handle_error(response.status)
                        raise Exception(f"Erreur API: {response.status}")
            
            except asyncio.TimeoutError:
                raise Exception("Timeout de la requête")
    
    def _is_valid_hotel_result(self, place_data: Dict[str, Any], hotel_name: str) -> bool:
        """Vérifie si le résultat correspond bien à un hôtel - Version finale propre
        
        Args:
            place_data (Dict[str, Any]): Données du lieu
            hotel_name (str): Nom de l'hôtel recherché
            
        Returns:
            bool: True si c'est un résultat valide
        """
        
        # Extraction intelligente du nom lisible
        place_name = self._extract_readable_name(place_data).lower()
        search_name = hotel_name.lower()
        
        # Si pas de nom, accepter (très permissif)
        if not place_name:
            return True
        
        # Nettoyer les noms pour comparaison
        clean_search = search_name.replace("hôtel", "").replace("hotel", "").replace("le ", "").replace("la ", "").strip()
        clean_place = place_name.replace("hôtel", "").replace("hotel", "").replace("le ", "").replace("la ", "").strip()
        
        # Validation intelligente - critères élargis
        validations = [
            # 1. Correspondance partielle des noms
            any(word in clean_place for word in clean_search.split() if len(word) > 2),
            # 2. Correspondance inverse  
            any(word in clean_search for word in clean_place.split() if len(word) > 2),
            # 3. Mots-clés hôteliers
            any(keyword in clean_place for keyword in ['plaza', 'brussels', 'hotel', 'hôtel']),
            # 4. Validation très permissive si types d'établissement correspondent
            any(hotel_type in place_data.get('types', []) for hotel_type in ['lodging', 'hotel'])
        ]
        
        return any(validations)
    
    def _extract_readable_name(self, place_data: Dict[str, Any]) -> str:
        """Extrait le nom lisible de l'hôtel depuis différentes sources
        
        Args:
            place_data (Dict[str, Any]): Données de l'API Google Maps
            
        Returns:
            str: Nom lisible de l'hôtel
        """
        
        # 1. Vérifier si le champ 'name' contient un nom lisible (pas un ID)
        name = place_data.get('name', '')
        if name and not name.startswith('places/'):
            return name
        
        # 2. Chercher dans les attributions des photos (source principale pour les noms d'hôtels)
        photos = place_data.get('photos', [])
        for photo in photos[:5]:  # Vérifier les 5 premières photos
            attributions = photo.get('authorAttributions', [])
            for attribution in attributions:
                display_name = attribution.get('displayName', '')
                if display_name and any(word in display_name.lower() for word in ['hotel', 'hôtel', 'plaza', 'brussels']):
                    return display_name
        
        # 3. Fallback: extraire depuis l'adresse
        address = place_data.get('formattedAddress', '')
        if address:
            # Prendre les premiers mots avant la première virgule
            parts = address.split(',')
            if parts:
                potential_name = parts[0].strip()
                if len(potential_name) > 5:  # Nom raisonnablement long
                    return potential_name
        
        # 4. Dernier fallback
        return ""
    
    def _format_hotel_data(self, place_data: Dict[str, Any], original_name: str, original_address: str) -> Dict[str, Any]:
        """Formate les données Google Maps selon le schéma attendu
        
        Args:
            place_data (Dict[str, Any]): Données brutes de l'API
            original_name (str): Nom original de l'hôtel
            original_address (str): Adresse originale
            
        Returns:
            Dict[str, Any]: Données formatées
        """
        
        # Extraction des heures d'ouverture
        opening_hours = self._format_opening_hours(place_data.get('regularOpeningHours'))
        
        # Extraction de l'image principale
        header_image_url = self._get_header_image_url(place_data.get('photos', []))
        
        # Extraction de la région/état
        region = self._extract_region_from_address(place_data.get('formattedAddress', ''))
        
        # Extraire le nom lisible intelligemment
        hotel_display_name = self._extract_readable_name(place_data) or original_name
        
        return {
            'input': f"{original_name} - {original_address}",
            'sharableLink': place_data.get('googleMapsUri', ''),
            'name': hotel_display_name,  # 🔧 CORRECTION: nom extrait intelligemment
            'isClosed': place_data.get('businessStatus') != 'OPERATIONAL',
            'website': place_data.get('websiteUri', ''),
            'category': self._get_primary_category(place_data.get('types', [])),
            'address': place_data.get('formattedAddress', original_address),
            'oloc': region,
            'averageRating': place_data.get('rating', 0),
            'reviewCount': place_data.get('userRatingCount', 0),
            'phoneNumber': place_data.get('nationalPhoneNumber', ''),
            'headerImageUrl': header_image_url,
            'openingHours': opening_hours,
            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'extraction_status': 'success'
        }
    
    def _format_opening_hours(self, opening_hours_data: Optional[Dict]) -> str:
        """Formate les heures d'ouverture
        
        Args:
            opening_hours_data (Optional[Dict]): Données d'heures d'ouverture
            
        Returns:
            str: Heures formatées
        """
        
        if not opening_hours_data or 'weekdayDescriptions' not in opening_hours_data:
            return ""
        
        descriptions = opening_hours_data['weekdayDescriptions']
        return "; ".join(descriptions) if descriptions else ""
    
    def _get_header_image_url(self, photos: List[Dict]) -> str:
        """Récupère l'URL de l'image principale
        
        Args:
            photos (List[Dict]): Liste des photos
            
        Returns:
            str: URL de l'image ou chaîne vide
        """
        
        if not photos:
            return ""
        
        # Prendre la première photo et construire l'URL
        first_photo = photos[0]
        name = first_photo.get('name', '')
        
        if name:
            # Format d'URL pour récupérer la photo (400px de large)
            return f"https://places.googleapis.com/v1/{name}/media?maxWidthPx=400&key={self.config.api_key}"
        
        return ""
    
    def _extract_region_from_address(self, address: str) -> str:
        """Extrait la région/état depuis l'adresse
        
        Args:
            address (str): Adresse complète
            
        Returns:
            str: Région/état
        """
        
        if not address:
            return ""
        
        # Logique simple : prendre le dernier élément après la dernière virgule
        parts = address.split(',')
        return parts[-1].strip() if len(parts) > 1 else ""
    
    def _get_primary_category(self, types: List[str]) -> str:
        """Détermine la catégorie principale du lieu
        
        Args:
            types (List[str]): Types du lieu
            
        Returns:
            str: Catégorie principale
        """
        
        # Hiérarchie des types préférés
        preferred_types = ['hotel', 'lodging', 'resort', 'motel', 'establishment']
        
        for preferred in preferred_types:
            if preferred in types:
                return preferred
        
        return types[0] if types else "hotel"
    
    def _create_empty_result(self, hotel_name: str, hotel_address: str, error_message: str) -> Dict[str, Any]:
        """Crée un résultat vide en cas d'échec
        
        Args:
            hotel_name (str): Nom de l'hôtel
            hotel_address (str): Adresse de l'hôtel
            error_message (str): Message d'erreur
            
        Returns:
            Dict[str, Any]: Résultat vide avec erreur
        """
        
        return {
            'input': f"{hotel_name} - {hotel_address}",
            'sharableLink': '',
            'name': hotel_name,
            'isClosed': False,
            'website': '',
            'category': 'hotel',
            'address': hotel_address,
            'oloc': '',
            'averageRating': 0,
            'reviewCount': 0,
            'phoneNumber': '',
            'headerImageUrl': '',
            'openingHours': '',
            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'extraction_status': 'failed',
            'error': error_message
        }


async def extract_hotels_batch(hotels_info: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Extrait les informations Google Maps pour un batch d'hôtels avec cache intelligent
    
    Args:
        hotels_info (List[Dict[str, str]]): Liste des infos hôtels (name, address)
        
    Returns:
        List[Dict[str, Any]]: Résultats d'extraction
    """
    
    # Initialiser le cache
    cache = await get_global_cache()
    extractor = GoogleMapsExtractor()
    results = []
    
    # Phase 1: Vérifier le cache pour tous les hôtels
    cache_results = await cache.batch_get(hotels_info)
    hotels_to_fetch = []
    hotels_to_cache = []
    
    print(f"🔍 Vérification cache pour {len(hotels_info)} hôtels...")
    
    for i, hotel_info in enumerate(hotels_info):
        cache_key = cache._generate_cache_key(hotel_info['name'], hotel_info['address'])
        cached_data = cache_results.get(cache_key)
        
        if cached_data:
            # Utiliser données cachées
            results.append(cached_data)
            print(f"💾 Cache HIT: {hotel_info['name']}")
        else:
            # Marquer pour extraction
            hotels_to_fetch.append((i, hotel_info))
            results.append(None)  # Placeholder
    
    # Phase 2: Extraire les hôtels manquants
    if hotels_to_fetch:
        print(f"🌐 Extraction API pour {len(hotels_to_fetch)} hôtels...")
        
        for original_index, hotel_info in hotels_to_fetch:
            try:
                result = await extractor.extract_hotel_info(
                    hotel_info['name'], 
                    hotel_info['address']
                )
                results[original_index] = result
                
                # Mettre en cache immédiatement si succès
                if result.get('success'):
                    await cache.set(hotel_info['name'], hotel_info['address'], result)
                    print(f"💾 Mis en cache: {hotel_info['name']}")
                
                # Reset des erreurs en cas de succès
                extractor.rate_limiter.reset_errors()
                
            except Exception as e:
                error_result = extractor._create_empty_result(
                    hotel_info['name'], 
                    hotel_info['address'], 
                    str(e)
                )
                results[original_index] = error_result
    
    # Statistiques cache
    if settings.cache.enable_cache:
        cache_hits = len([r for r in cache_results.values() if r is not None])
        cache_misses = len(hotels_to_fetch)
        hit_rate = (cache_hits / len(hotels_info) * 100) if hotels_info else 0
        
        print(f"📊 Cache: {cache_hits} hits, {cache_misses} misses ({hit_rate:.1f}% hit rate)")
        
        # Économies API
        api_savings = cache_hits
        if api_savings > 0:
            print(f"💰 API économisées: {api_savings} requêtes ({hit_rate:.1f}% réduction)")
    
    return results 