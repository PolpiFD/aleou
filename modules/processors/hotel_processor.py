"""
Processeur individuel d'hôtel - Gère l'extraction pour un hôtel unique
"""

import asyncio
import time
from typing import Dict, Any, Optional
from datetime import datetime

from ..cvent_extractor import extract_cvent_data
from ..gmaps_extractor import extract_hotels_batch
from ..website_extractor import extract_hotels_websites_batch
from config.settings import settings


class HotelProcessor:
    """Processeur pour un hôtel individuel"""
    
    def __init__(self):
        self.stats = {
            'cvent_time': 0,
            'gmaps_time': 0, 
            'website_time': 0,
            'total_time': 0,
            'errors': []
        }
    
    async def process_hotel(self, hotel_data: Dict[str, Any], 
                          enable_cvent: bool = True,
                          enable_gmaps: bool = True, 
                          enable_website: bool = True) -> Dict[str, Any]:
        """
        Traite un hôtel unique avec toutes les extractions demandées
        
        Args:
            hotel_data: Données de base de l'hôtel
            enable_cvent: Activer extraction Cvent
            enable_gmaps: Activer extraction Google Maps
            enable_website: Activer extraction website
            
        Returns:
            Dict avec toutes les données extraites
        """
        start_time = time.time()
        result = {
            'hotel_data': hotel_data,
            'cvent_data': None,
            'gmaps_data': None,
            'website_data': None,
            'success': False,
            'errors': [],
            'processing_time': 0,
            'timestamp': datetime.now().isoformat()
        }
        
        hotel_name = hotel_data.get('name', 'Unknown')
        print(f"🏨 Traitement de: {hotel_name}")
        
        try:
            # Extraction Cvent
            if enable_cvent:
                result['cvent_data'] = await self._extract_cvent(hotel_data)
            
            # Extraction Google Maps 
            if enable_gmaps:
                result['gmaps_data'] = await self._extract_gmaps(hotel_data)
            
            # Extraction Website (utilise gmaps si disponible)
            if enable_website:
                gmaps_website = None
                if result['gmaps_data'] and result['gmaps_data'].get('success'):
                    gmaps_website = result['gmaps_data'].get('website')
                
                result['website_data'] = await self._extract_website(hotel_data, gmaps_website)
            
            # Déterminer le succès global
            result['success'] = self._calculate_success(result, enable_cvent, enable_gmaps, enable_website)
            
        except Exception as e:
            error_msg = f"Erreur traitement {hotel_name}: {str(e)}"
            result['errors'].append(error_msg)
            print(f"❌ {error_msg}")
        
        # Statistiques finales
        result['processing_time'] = time.time() - start_time
        self.stats['total_time'] = result['processing_time']
        
        status = "✅" if result['success'] else "❌"
        print(f"{status} {hotel_name}: {result['processing_time']:.1f}s")
        
        return result
    
    async def _extract_cvent(self, hotel_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extraction des données Cvent"""
        start_time = time.time()
        try:
            cvent_data = await asyncio.wait_for(
                extract_cvent_data(hotel_data),
                timeout=settings.scraping.playwright_timeout / 1000  # Convert to seconds
            )
            self.stats['cvent_time'] = time.time() - start_time
            return cvent_data
        except asyncio.TimeoutError:
            error_msg = f"Timeout Cvent pour {hotel_data.get('name')}"
            self.stats['errors'].append(error_msg)
            print(f"⏱️ {error_msg}")
            return None
        except Exception as e:
            error_msg = f"Erreur Cvent: {str(e)}"
            self.stats['errors'].append(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    async def _extract_gmaps(self, hotel_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extraction des données Google Maps"""
        start_time = time.time()
        try:
            # extract_hotels_batch attend une liste
            gmaps_results = await asyncio.wait_for(
                extract_hotels_batch([hotel_data]),
                timeout=30
            )
            self.stats['gmaps_time'] = time.time() - start_time
            
            # Retourner le premier résultat
            if gmaps_results and len(gmaps_results) > 0:
                return gmaps_results[0]
            return None
            
        except asyncio.TimeoutError:
            error_msg = f"Timeout Google Maps pour {hotel_data.get('name')}"
            self.stats['errors'].append(error_msg)
            print(f"⏱️ {error_msg}")
            return None
        except Exception as e:
            error_msg = f"Erreur Google Maps: {str(e)}"
            self.stats['errors'].append(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    async def _extract_website(self, hotel_data: Dict[str, Any], 
                             gmaps_website: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Extraction des données website"""
        start_time = time.time()
        try:
            # Préparer les données avec website Google Maps si disponible
            website_hotel_data = hotel_data.copy()
            if gmaps_website:
                website_hotel_data['gmaps_website'] = gmaps_website
            
            website_results = await asyncio.wait_for(
                extract_hotels_websites_batch([website_hotel_data]),
                timeout=60
            )
            self.stats['website_time'] = time.time() - start_time
            
            # Retourner le premier résultat
            if website_results and len(website_results) > 0:
                return website_results[0]
            return None
            
        except asyncio.TimeoutError:
            error_msg = f"Timeout Website pour {hotel_data.get('name')}"
            self.stats['errors'].append(error_msg)
            print(f"⏱️ {error_msg}")
            return None
        except Exception as e:
            error_msg = f"Erreur Website: {str(e)}"
            self.stats['errors'].append(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    def _calculate_success(self, result: Dict[str, Any], 
                         enable_cvent: bool, enable_gmaps: bool, enable_website: bool) -> bool:
        """Calcule si le traitement est considéré comme réussi"""
        success_count = 0
        total_enabled = 0
        
        if enable_cvent:
            total_enabled += 1
            if result['cvent_data'] and result['cvent_data'].get('success'):
                success_count += 1
        
        if enable_gmaps:
            total_enabled += 1  
            if result['gmaps_data'] and result['gmaps_data'].get('success'):
                success_count += 1
        
        if enable_website:
            total_enabled += 1
            if result['website_data'] and result['website_data'].get('success'):
                success_count += 1
        
        # Succès si au moins 1 extraction réussie
        return success_count > 0 and total_enabled > 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de traitement"""
        return self.stats.copy()