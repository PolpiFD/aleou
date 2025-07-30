"""
Module de recherche de sites web officiels d'hôtels
Utilise UNIQUEMENT les données Google Maps - Suppression autom.dev
"""

import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class WebsiteFinderConfig:
    """Configuration simplifiée pour la recherche de sites web"""
    timeout: int = 10
    
    @classmethod
    def from_env(cls):
        """Crée la configuration par défaut"""
        return cls()


class WebsiteFinder:
    """Trouveur de sites web officiels basé UNIQUEMENT sur Google Maps"""
    
    def __init__(self, config: WebsiteFinderConfig = None):
        self.config = config or WebsiteFinderConfig.from_env()
    
    async def find_official_website(self, hotel_name: str, hotel_address: str, gmaps_website: str = None) -> Dict[str, Any]:
        """Trouve le site web officiel d'un hôtel depuis Google Maps UNIQUEMENT
        
        Args:
            hotel_name (str): Nom de l'hôtel
            hotel_address (str): Adresse de l'hôtel
            gmaps_website (str): Site web depuis Google Maps (OBLIGATOIRE)
            
        Returns:
            Dict[str, Any]: Résultat avec URL trouvée et métadonnées
        """
        
        # NOUVEAU WORKFLOW : Google Maps UNIQUEMENT
        if gmaps_website and self._is_valid_website_url(gmaps_website):
            print(f"✅ Site web Google Maps trouvé pour {hotel_name}: {gmaps_website}")
            return self._create_success_result(gmaps_website, 'google_maps', hotel_name)
        
        # Si pas de site Google Maps valide = ÉCHEC
        error_msg = "Aucun site web Google Maps disponible" if not gmaps_website else "Site web Google Maps invalide"
        print(f"❌ {error_msg} pour {hotel_name}")
        return self._create_failure_result(hotel_name, error_msg)
    
    def _is_valid_website_url(self, url: str) -> bool:
        """Valide une URL de site web
        
        Args:
            url (str): URL à valider
            
        Returns:
            bool: True si l'URL est valide
        """
        
        if not url or not isinstance(url, str):
            return False
        
        # Nettoyer l'URL des espaces
        url = url.strip()
        if not url:
            return False
        
        try:
            parsed = urlparse(url)
            
            # Vérifications de base
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # URLs à exclure (réseaux sociaux et plateformes de booking)
            excluded_domains = [
                'facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com',
                'booking.com', 'expedia.com', 'tripadvisor.com', 'hotels.com',
                'google.com', 'maps.google.com', 'youtube.com', 'agoda.com',
                'priceline.com', 'orbitz.com', 'travelocity.com', 'kayak.com'
            ]
            
            domain = parsed.netloc.lower()
            # Vérification précise : domaine exact ou sous-domaine
            is_excluded = any(
                domain == excluded or domain.endswith('.' + excluded)
                for excluded in excluded_domains
            )
            
            if is_excluded:
                print(f"⚠️ URL exclue (plateforme tierce): {domain}")
                return False
            
            return True
            
        except Exception as e:
            print(f"⚠️ Erreur validation URL: {e}")
            return False
    
    def _create_success_result(self, url: str, source: str, hotel_name: str) -> Dict[str, Any]:
        """Crée un résultat de succès standardisé
        
        Args:
            url (str): URL trouvée
            source (str): Source de l'URL (toujours 'google_maps')
            hotel_name (str): Nom de l'hôtel
            
        Returns:
            Dict[str, Any]: Résultat de succès
        """
        
        return {
            'success': True,
            'hotel_name': hotel_name,
            'website_url': url,
            'source': source,
            'error': None
        }
    
    def _create_failure_result(self, hotel_name: str, error_message: str) -> Dict[str, Any]:
        """Crée un résultat d'échec standardisé
        
        Args:
            hotel_name (str): Nom de l'hôtel
            error_message (str): Message d'erreur
            
        Returns:
            Dict[str, Any]: Résultat d'échec
        """
        
        return {
            'success': False,
            'hotel_name': hotel_name,
            'website_url': None,
            'source': None,
            'error': error_message
        }


async def find_websites_batch(hotels_info: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Trouve les sites web pour un batch d'hôtels (Google Maps uniquement)
    
    Args:
        hotels_info (List[Dict[str, str]]): Liste des infos hôtels avec gmaps_website
        
    Returns:
        List[Dict[str, Any]]: Résultats de recherche
    """
    
    print(f"🔍 Recherche sites web Google Maps: {len(hotels_info)} hôtels")
    
    results = []
    finder = WebsiteFinder()
    
    for hotel_info in hotels_info:
        try:
            result = await finder.find_official_website(
                hotel_info['name'],
                hotel_info.get('address', ''),
                hotel_info.get('gmaps_website')
            )
            results.append(result)
            
        except Exception as e:
            error_result = finder._create_failure_result(
                hotel_info['name'],
                f"Erreur: {str(e)}"
            )
            results.append(error_result)
    
    success_count = sum(1 for r in results if r['success'])
    print(f"✅ Recherche Google Maps terminée: {success_count}/{len(hotels_info)} succès")
    
    return results