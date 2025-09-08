"""
Processeur de sites web unifié - Intégration Firecrawl dans l'architecture existante
Remplace l'ancien LLM processor avec des capacités avancées
"""

import asyncio
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict

from .firecrawl_extractor import FirecrawlExtractor, FirecrawlConfig, extract_hotels_with_firecrawl
# 🔥 FIRECRAWL ONLY - Plus de Legacy imports


@dataclass 
class WebsiteProcessorConfig:
    """Configuration du processeur de sites web"""
    use_firecrawl: bool = True
    firecrawl_api_key: str = ""
    fallback_to_legacy: bool = False  # 🔥 FIRECRAWL ONLY
    max_concurrent_extractions: int = 3
    batch_size: int = 8
    timeout: int = 120
    
    @classmethod
    def from_env(cls):
        """Crée la configuration depuis les variables d'environnement"""
        return cls(
            use_firecrawl=True,  # 🔧 Réactiver Firecrawl pour debug NoneType
            firecrawl_api_key=os.getenv('FIRECRAWL_API_KEY', ''),
            fallback_to_legacy=False,  # 🔧 Désactiver Legacy pour forcer Firecrawl
            max_concurrent_extractions=int(os.getenv('MAX_CONCURRENT_EXTRACTIONS', '3')),
            batch_size=int(os.getenv('FIRECRAWL_BATCH_SIZE', '8')),
            timeout=int(os.getenv('FIRECRAWL_TIMEOUT', '120'))
        )


class WebsiteProcessor:
    """Processeur unifié pour l'extraction de données de sites web d'hôtels
    
    Utilise Firecrawl en priorité avec fallback sur l'ancien système LLM
    """
    
    def __init__(self, config: WebsiteProcessorConfig = None):
        self.config = config or WebsiteProcessorConfig.from_env()
        
        # Initialiser Firecrawl si disponible
        self.firecrawl_extractor = None
        if self.config.use_firecrawl and self.config.firecrawl_api_key:
            try:
                firecrawl_config = FirecrawlConfig(
                    api_key=self.config.firecrawl_api_key,
                    rate_limit_requests_per_minute=100,  # Plan payant
                    rate_limit_wait_seconds=1,  # Sécurité 1s
                    timeout=self.config.timeout
                )
                self.firecrawl_available = True
                print("🔥 Firecrawl configuré et prêt")
            except Exception as e:
                print(f"⚠️ Erreur configuration Firecrawl: {e}")
                self.firecrawl_available = False
        else:
            self.firecrawl_available = False
            print("📝 Firecrawl non disponible - utilisation Legacy LLM")
        
        # Legacy supprimé - Firecrawl uniquement
        self.legacy_available = False
        
        # Statistiques
        self.stats = {
            'total_processed': 0,
            'firecrawl_success': 0,
            'legacy_fallback': 0,
            'total_failures': 0,
            'processing_start': None
        }
    
    async def process_hotels_websites(self, hotels_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Traite les sites web d'une liste d'hôtels
        
        Args:
            hotels_data: Liste de dictionnaires avec name, address, website_url ou url
            
        Returns:
            Liste des résultats de traitement
        """
        
        print(f"🌐 Début traitement sites web pour {len(hotels_data)} hôtels")
        self.stats['processing_start'] = datetime.now()
        self.stats['total_processed'] = len(hotels_data)
        
        # Normaliser les données d'entrée
        normalized_hotels = self._normalize_hotel_data(hotels_data)
        
        # Séparer les hôtels avec/sans URL
        hotels_with_urls = [h for h in normalized_hotels if h.get('website_url')]
        hotels_without_urls = [h for h in normalized_hotels if not h.get('website_url')]
        
        print(f"📊 {len(hotels_with_urls)} hôtels avec URL, {len(hotels_without_urls)} sans URL")
        
        results = []
        
        # Traiter les hôtels avec URLs
        if hotels_with_urls:
            if self.firecrawl_available and self.config.use_firecrawl:
                print("🔥 Utilisation Firecrawl pour extraction")
                firecrawl_results = await self._process_with_firecrawl(hotels_with_urls)
                results.extend(firecrawl_results)
            elif self.legacy_available:
                print("🔄 Utilisation Legacy pour extraction (temporaire)")
                legacy_results = await self._process_with_legacy(hotels_with_urls)
                results.extend(legacy_results)
            else:
                print("❌ Aucun processeur disponible")
                for hotel in hotels_with_urls:
                    results.append(self._create_failure_result(hotel, "Aucun processeur disponible"))
        
        # Traiter les hôtels sans URLs (résultats vides)
        for hotel in hotels_without_urls:
            results.append(self._create_no_url_result(hotel))
        
        # Log final
        processing_time = (datetime.now() - self.stats['processing_start']).total_seconds()
        print(f"✅ Traitement terminé en {processing_time:.1f}s")
        print(f"   🔥 Firecrawl: {self.stats['firecrawl_success']}")
        print(f"   🔄 Legacy: {self.stats['legacy_fallback']}")
        print(f"   ❌ Échecs: {self.stats['total_failures']}")
        
        return results
    
    async def _process_with_firecrawl(self, hotels_data: List[Dict]) -> List[Dict[str, Any]]:
        """Traite avec Firecrawl"""
        
        try:
            firecrawl_config = FirecrawlConfig(
                api_key=self.config.firecrawl_api_key,
                rate_limit_requests_per_minute=100,  # Plan payant
                rate_limit_wait_seconds=1,  # Sécurité 1s
                timeout=self.config.timeout
            )
            
            async with FirecrawlExtractor(firecrawl_config) as extractor:
                results = await extractor.extract_hotels_batch(hotels_data)
            
            # 🔥 FIRECRAWL ONLY - Pas de fallback Legacy
            processed_results = []
            for result in results:
                if result['success']:
                    self.stats['firecrawl_success'] += 1
                else:
                    self.stats['total_failures'] += 1
                processed_results.append(self._format_firecrawl_result(result))
            
            return processed_results
            
        except Exception as e:
            print(f"❌ Erreur Firecrawl globale: {e}")
            # 🔥 PAS DE FALLBACK - Retourner des échecs
            return [self._create_failure_result(hotel, str(e)) for hotel in hotels_data]
    
    async def _process_with_legacy(self, hotels_data: List[Dict]) -> List[Dict[str, Any]]:
        """Traite avec Legacy system (temporaire)"""
        
        print("🔄 Traitement Legacy temporaire...")
        results = []
        
        for hotel in hotels_data:
            try:
                # Extraction basic legacy
                result = {
                    'success': True,
                    'hotel_name': hotel['name'],
                    'hotel_address': hotel.get('address', ''),
                    'website_data': self._create_empty_hotel_data(),
                    'metadata': {
                        'url': hotel.get('website_url', ''),
                        'extraction_method': 'legacy_placeholder',
                        'processing_date': datetime.now().isoformat()
                    },
                    'error': None
                }
                results.append(result)
                self.stats['legacy_fallback'] += 1
                
            except Exception as e:
                results.append(self._create_failure_result(hotel, str(e)))
                self.stats['total_failures'] += 1
        
        return results
    
    def _normalize_hotel_data(self, hotels_data: List[Dict]) -> List[Dict]:
        """Normalise les données d'hôtels pour traitement uniforme"""
        
        normalized = []
        for hotel in hotels_data:
            normalized_hotel = {
                'name': hotel.get('name', hotel.get('hotel_name', 'Hotel_Unknown')),
                'address': hotel.get('address', hotel.get('hotel_address', '')),
                'website_url': hotel.get('website_url', hotel.get('url', ''))
            }
            
            # Validation URL
            website_url = normalized_hotel['website_url']
            if website_url and not website_url.startswith(('http://', 'https://')):
                if '.' in website_url:  # Semble être un domaine
                    normalized_hotel['website_url'] = f"https://{website_url}"
                else:
                    normalized_hotel['website_url'] = ''  # URL invalide
            
            normalized.append(normalized_hotel)
        
        return normalized
    
    def _format_firecrawl_result(self, firecrawl_result: Dict) -> Dict[str, Any]:
        """Formate le résultat Firecrawl pour compatibilité avec le système existant"""
        
        if not firecrawl_result['success']:
            return firecrawl_result
        
        # Mapper les données Firecrawl vers le format attendu par le consolidator
        website_data = firecrawl_result.get('website_data', {})
        
        # Format compatible avec data_consolidator.py
        formatted_result = {
            'success': True,
            'hotel_name': firecrawl_result['hotel_name'],
            'hotel_address': firecrawl_result['hotel_address'],
            'website_data': website_data,  # Déjà au bon format
            'metadata': firecrawl_result.get('metadata', {}),
            'error': None
        }
        
        return formatted_result
    
    def _create_empty_hotel_data(self) -> Dict[str, Any]:
        """Crée un jeu de données hôtel vide pour Firecrawl"""
        
        # 🔥 Structure vide compatible Firecrawl
        return {
            'capacite_max': None,
            'nombre_chambre': None,
            'nombre_etoile': None,
            'pr_parking': None,
            'pr_restaurant': None,
            'pr_wifi': None,
            'summary': '',
            'hotel_phone': None,
            'hotel_email': None,
            'meeting_rooms_available': None
        }
    
    def _create_no_url_result(self, hotel: Dict) -> Dict[str, Any]:
        """Crée un résultat pour un hôtel sans URL"""
        
        return {
            'success': False,
            'hotel_name': hotel['name'],
            'hotel_address': hotel.get('address', ''),
            'website_data': self._create_empty_hotel_data(),
            'metadata': {
                'url': '',
                'extraction_method': 'none',
                'processing_date': datetime.now().isoformat()
            },
            'error': 'Aucune URL de site web fournie'
        }
    
    def _create_failure_result(self, hotel: Dict, error_message: str) -> Dict[str, Any]:
        """Crée un résultat d'échec"""
        
        return {
            'success': False,
            'hotel_name': hotel['name'],
            'hotel_address': hotel.get('address', ''),
            'website_data': self._create_empty_hotel_data(),
            'metadata': {
                'url': hotel.get('website_url', ''),
                'extraction_method': 'failed',
                'processing_date': datetime.now().isoformat()
            },
            'error': error_message
        }


# Fonction d'usage pratique pour compatibilité
async def process_hotels_websites(hotels_data: List[Dict[str, Any]], 
                                 config: WebsiteProcessorConfig = None) -> List[Dict[str, Any]]:
    """Fonction utilitaire pour traiter des sites web d'hôtels
    
    Args:
        hotels_data: Liste de données hôtels avec name, address, website_url
        config: Configuration optionnelle
        
    Returns:
        Liste des résultats de traitement
    """
    
    processor = WebsiteProcessor(config)
    return await processor.process_hotels_websites(hotels_data)


# 🔥 FONCTION DE COMPATIBILITÉ SUPPRIMÉE - Firecrawl only
# Plus besoin de process_content_batch legacy