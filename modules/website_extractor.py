"""
Module d'extraction d'informations depuis les sites web d'hôtels
Orchestrateur principal qui coordonne la recherche, le scraping et le traitement LLM
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime

from .website_finder import WebsiteFinder, find_websites_batch
from .website_processor import WebsiteProcessor, process_hotels_websites
import aiohttp
from bs4 import BeautifulSoup


class WebsiteExtractor:
    """Extracteur principal pour les informations de sites web d'hôtels
    
    🔥 NOUVEAU: Utilise Firecrawl en priorité avec fallback Legacy
    """
    
    def __init__(self):
        self.website_finder = WebsiteFinder()
        # 🔥 Nouveau processeur unifié (Firecrawl + Legacy)
        self.website_processor = None
        
        # Session aiohttp pour fallback simple
        self.session = None
    
    async def extract_hotel_website_data(self, hotel_name: str, hotel_address: str, 
                                       gmaps_website: str = None) -> Dict[str, Any]:
        """Extrait les données complètes d'un site web d'hôtel
        
        🔥 NOUVEAU: Utilise Firecrawl via website_processor
        
        Args:
            hotel_name (str): Nom de l'hôtel
            hotel_address (str): Adresse de l'hôtel  
            gmaps_website (str): Site web depuis Google Maps (prioritaire)
            
        Returns:
            Dict[str, Any]: Données complètes extraites du site web
        """
        
        print(f"🌐 Début extraction website pour {hotel_name} (via Firecrawl)")
        
        try:
            # Étape 1: Trouver le site web officiel (inchangé)
            website_result = await self.website_finder.find_official_website(
                hotel_name, hotel_address, gmaps_website
            )
            
            if not website_result['success']:
                return {
                    'hotel_name': hotel_name,
                    'hotel_address': hotel_address,
                    'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'success': False,
                    'error': f"Site web non trouvé: {website_result['error']}",
                    'website_data': None
                }
            
            website_url = website_result['website_url']
            print(f"✅ Site web trouvé: {website_url}")
            
            # 🔥 NOUVEAU: Utiliser website_processor (Firecrawl + fallback Legacy)
            hotel_data = [{
                'name': hotel_name,
                'address': hotel_address,
                'website_url': website_url
            }]
            
            extraction_results = await process_hotels_websites(hotel_data)
            
            if extraction_results and len(extraction_results) > 0:
                result = extraction_results[0]
                # Format compatible avec l'ancien système
                if result['success']:
                    print(f"🎉 Extraction website réussie pour {hotel_name} (Firecrawl)")
                else:
                    print(f"⚠️ Extraction Firecrawl échouée, tentative Legacy...")
                    # Fallback vers l'ancien système si nécessaire
                    result = await self._legacy_extraction(hotel_name, hotel_address, website_url)
                
                return result
            else:
                raise Exception("Aucun résultat d'extraction")
                
        except Exception as e:
            print(f"❌ Erreur extraction website {hotel_name}: {e}")
            return {
                'hotel_name': hotel_name,
                'hotel_address': hotel_address,
                'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'success': False,
                'error': f"Erreur générale: {str(e)}",
                'website_data': None
            }
    
    async def _legacy_extraction(self, hotel_name: str, hotel_address: str, website_url: str) -> Dict[str, Any]:
        """Fallback vers extraction simple aiohttp (plus de Playwright)"""
        
        print(f"🔄 Fallback aiohttp simple pour {hotel_name}")
        
        try:
            # Étape 2: Scraper le contenu avec aiohttp simple (plus de Playwright)
            scraping_result = await self._simple_aiohttp_scrape(website_url, hotel_name)
            
            if not scraping_result['success']:
                raise Exception(f"Erreur scraping aiohttp: {scraping_result['error']}")
            
            print(f"✅ Contenu scrapé (aiohttp): {len(scraping_result['text_content'])} caractères")
            
            # Retourner données basiques sans LLM processing (fallback simple)
            basic_data = {
                'website_url': website_url,
                'source': 'aiohttp_fallback',
                'content_length': len(scraping_result['text_content']),
                'images_found': len(scraping_result['image_urls']),
                'summary': f"Données extraites par aiohttp ({len(scraping_result['text_content'])} caractères)",
                'extraction_method': 'aiohttp_simple'
            }
            
            return {
                'hotel_name': hotel_name,
                'hotel_address': hotel_address,
                'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'success': True,
                'website_data': basic_data,
                'error': None
            }
            
        except Exception as e:
            return {
                'hotel_name': hotel_name,
                'hotel_address': hotel_address,
                'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'success': False,
                'error': f"Legacy extraction failed: {str(e)}",
                'website_data': None
            }
    
    async def _simple_aiohttp_scrape(self, url: str, hotel_name: str) -> Dict[str, Any]:
        """Scraping simple avec aiohttp + BeautifulSoup (remplace Playwright)"""
        
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return {
                        'success': False,
                        'error': f'HTTP {response.status}',
                        'text_content': '',
                        'image_urls': []
                    }
                
                html_content = await response.text()
                
                # Parser avec BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Supprimer scripts et styles
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Extraire le texte
                text_content = soup.get_text()
                
                # Nettoyer le texte
                lines = (line.strip() for line in text_content.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text_content = ' '.join(chunk for chunk in chunks if chunk)
                
                # Extraire les images
                images = soup.find_all('img')
                image_urls = []
                for img in images[:10]:  # Limiter à 10 images
                    src = img.get('src')
                    if src and src.startswith(('http', '/')):
                        if src.startswith('/'):
                            from urllib.parse import urljoin
                            src = urljoin(url, src)
                        image_urls.append(src)
                
                return {
                    'success': True,
                    'text_content': text_content,
                    'image_urls': image_urls,
                    'error': None
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'aiohttp scraping failed: {str(e)}',
                'text_content': '',
                'image_urls': []
            }
    
    async def __aenter__(self):
        """Context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
    
    
    async def extract_hotels_batch(self, hotels_info: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Extrait les données website pour un batch d'hôtels
        
        Args:
            hotels_info (List[Dict[str, str]]): Liste des infos hôtels avec gmaps_website
            
        Returns:
            List[Dict[str, Any]]: Résultats d'extraction
        """
        
        print(f"🌐 Début extraction batch website: {len(hotels_info)} hôtels")
        
        # Étape 1: Recherche des sites web (peut être parallélisée)
        print("🔍 Phase 1: Recherche des sites web...")
        website_results = await find_websites_batch(hotels_info)
        
        # Préparer les données pour le scraping
        websites_to_scrape = []
        for i, website_result in enumerate(website_results):
            if website_result['success']:
                websites_to_scrape.append({
                    'name': website_result['hotel_name'],
                    'website_url': website_result['website_url'],
                    'index': i
                })
        
        print(f"✅ Sites web trouvés: {len(websites_to_scrape)}/{len(hotels_info)}")
        
        # Étape 2: Scraping du contenu (parallélisé)
        print("🕷️ Phase 2: Scraping du contenu...")
        scraping_results = await scrape_websites_batch(websites_to_scrape)
        
        # Préparer les données pour le LLM
        contents_to_process = []
        scraping_index_map = {}
        
        for i, scraping_result in enumerate(scraping_results):
            contents_to_process.append(scraping_result)
            original_index = websites_to_scrape[i]['index']
            scraping_index_map[i] = original_index
        
        print(f"✅ Contenu scrapé: {sum(1 for r in scraping_results if r['success'])}/{len(scraping_results)}")
        
        # Étape 3: Traitement LLM (parallélisé mais avec rate limiting)
        print("🧠 Phase 3: Traitement LLM...")
        llm_results = await process_content_batch(contents_to_process)
        
        print(f"✅ Traitement LLM: {sum(1 for r in llm_results if r['success'])}/{len(llm_results)}")
        
        # Étape 4: Compiler les résultats finaux
        final_results = []
        
        for i, hotel_info in enumerate(hotels_info):
            hotel_name = hotel_info['name']
            
            # Initialiser avec un résultat d'échec par défaut
            result = {
                'hotel_name': hotel_name,
                'hotel_address': hotel_info.get('address', ''),
                'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'success': False,
                'error': None,
                'website_data': None
            }
            
            # Vérifier si on a des données pour cet hôtel
            website_result = website_results[i] if i < len(website_results) else None
            
            if website_result and website_result['success']:
                # Trouver les résultats correspondants dans les autres étapes
                scraping_result = None
                llm_result = None
                
                # Chercher dans les résultats de scraping
                for scraping_idx, orig_idx in scraping_index_map.items():
                    if orig_idx == i:
                        scraping_result = scraping_results[scraping_idx]
                        if scraping_idx < len(llm_results):
                            llm_result = llm_results[scraping_idx]
                        break
                
                if scraping_result and scraping_result['success'] and llm_result and llm_result['success']:
                    # Succès complet
                    result['success'] = True
                    result['website_data'] = self._compile_website_data(
                        website_result, scraping_result, llm_result
                    )
                else:
                    # Échec partiel
                    if not scraping_result or not scraping_result['success']:
                        result['error'] = f"Erreur scraping: {scraping_result['error'] if scraping_result else 'Pas de données'}"
                    elif not llm_result or not llm_result['success']:
                        result['error'] = f"Erreur LLM: {llm_result['error'] if llm_result else 'Pas de données'}"
            else:
                # Échec de recherche du site web
                result['error'] = f"Site web non trouvé: {website_result['error'] if website_result else 'Pas de données'}"
            
            final_results.append(result)
        
        success_count = sum(1 for r in final_results if r['success'])
        print(f"🎉 Extraction website batch terminée: {success_count}/{len(hotels_info)} succès")
        
        return final_results


def format_website_data_for_consolidation(website_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Formate les résultats website pour la consolidation CSV
    
    Args:
        website_results (List[Dict[str, Any]]): Résultats bruts d'extraction website
        
    Returns:
        List[Dict[str, Any]]: Résultats formatés pour consolidation
    """
    
    formatted_results = []
    
    for result in website_results:
        if result['success'] and result['website_data']:
            # Créer un objet formaté pour la consolidation
            formatted_result = {
                'name': result['hotel_name'],
                'address': result['hotel_address'],
                'extraction_date': result['extraction_date'],
                'success': True,
                'website_data': result['website_data']
            }
        else:
            # Résultat d'échec
            formatted_result = {
                'name': result['hotel_name'],
                'address': result['hotel_address'],
                'extraction_date': result['extraction_date'],
                'success': False,
                'error': result.get('error', 'Erreur inconnue'),
                'website_data': None
            }
        
        formatted_results.append(formatted_result)
    
    return formatted_results


async def extract_single_hotel_website(hotel_name: str, hotel_address: str, 
                                     gmaps_website: str = None) -> Dict[str, Any]:
    """Fonction utilitaire pour extraire les données d'un seul hôtel
    
    Args:
        hotel_name (str): Nom de l'hôtel
        hotel_address (str): Adresse de l'hôtel
        gmaps_website (str): Site web depuis Google Maps
        
    Returns:
        Dict[str, Any]: Résultat d'extraction
    """
    
    extractor = WebsiteExtractor()
    return await extractor.extract_hotel_website_data(hotel_name, hotel_address, gmaps_website)


async def extract_hotels_websites_batch(hotels_info: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Fonction utilitaire pour extraire les données d'un batch d'hôtels
    
    🔥 NOUVEAU: Utilise directement le website_processor (plus efficace)
    
    Args:
        hotels_info (List[Dict[str, str]]): Liste des infos hôtels
        
    Returns:
        List[Dict[str, Any]]: Résultats d'extraction
    """
    
    print(f"🚀 Extraction batch optimisée (Firecrawl) pour {len(hotels_info)} hôtels")
    
    try:
        # 🔥 NOUVEAU: Traitement direct avec Firecrawl (plus rapide)
        
        # Étape 1: Trouver tous les sites web d'abord
        print("🔍 Phase 1: Recherche des sites web...")
        website_results = await find_websites_batch(hotels_info)
        
        # Préparer les données pour Firecrawl
        hotels_with_websites = []
        hotels_mapping = {}  # Pour retrouver les indices originaux
        
        for i, (hotel_info, website_result) in enumerate(zip(hotels_info, website_results)):
            if website_result['success']:
                hotel_data = {
                    'name': hotel_info['name'],
                    'address': hotel_info.get('address', ''),
                    'website_url': website_result['website_url']
                }
                hotels_with_websites.append(hotel_data)
                hotels_mapping[len(hotels_with_websites) - 1] = i
        
        print(f"✅ Sites web trouvés: {len(hotels_with_websites)}/{len(hotels_info)}")
        
        # Étape 2: Extraction avec Firecrawl
        if hotels_with_websites:
            print("🔥 Phase 2: Extraction Firecrawl...")
            firecrawl_results = await process_hotels_websites(hotels_with_websites)
        else:
            firecrawl_results = []
        
        # Étape 3: Reconstruire les résultats complets dans l'ordre original
        final_results = []
        
        for i, hotel_info in enumerate(hotels_info):
            # Trouver le résultat correspondant
            result = None
            
            # Chercher dans les résultats Firecrawl
            for firecrawl_idx, original_idx in hotels_mapping.items():
                if original_idx == i and firecrawl_idx < len(firecrawl_results):
                    result = firecrawl_results[firecrawl_idx]
                    break
            
            if not result:
                # Créer un résultat d'échec
                website_result = website_results[i] if i < len(website_results) else None
                error_msg = website_result['error'] if website_result and not website_result['success'] else "Pas de site web"
                
                result = {
                    'hotel_name': hotel_info['name'],
                    'hotel_address': hotel_info.get('address', ''),
                    'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'success': False,
                    'error': error_msg,
                    'website_data': None
                }
            
            final_results.append(result)
        
        success_count = sum(1 for r in final_results if r['success'])
        print(f"🎉 Extraction batch terminée: {success_count}/{len(hotels_info)} succès")
        
        return format_website_data_for_consolidation(final_results)
        
    except Exception as e:
        print(f"❌ Erreur extraction batch: {e}")
        # Fallback vers l'ancien système
        print("🔄 Fallback vers l'ancien système...")
        extractor = WebsiteExtractor()
        results = await extractor.extract_hotels_batch(hotels_info)
        return format_website_data_for_consolidation(results)