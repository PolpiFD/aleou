"""
Module d'extraction Firecrawl pour données hôtelières
Architecture parallélisée et robuste avec gestion d'erreurs avancée
"""

import os
import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from pydantic import BaseModel, Field
import json
from pathlib import Path

try:
    from firecrawl import FirecrawlApp
except ImportError:
    print("⚠️ Firecrawl-py non installé. Installez avec: pip install firecrawl-py")
    FirecrawlApp = None

from config.settings import settings


class HotelFirecrawlSchema(BaseModel):
    """Schéma Pydantic pour extraction Firecrawl des données hôtelières"""
    
    # Données quantitatives principales
    capacite_max: Optional[int] = Field(None, description="Capacité maximale totale de l'hôtel pour événements")
    nombre_chambre: Optional[int] = Field(None, description="Nombre total de chambres dans l'hôtel")
    nombre_chambre_twin: Optional[int] = Field(None, description="Nombre de chambres twin/doubles")
    nombre_etoile: Optional[int] = Field(None, description="Nombre d'étoiles de l'hôtel (1-5)")
    
    # Capacités spécialisées pour événements d'affaires
    pr_amphi: Optional[int] = Field(None, description="Capacité amphithéâtre/auditorium")
    pr_hotel: Optional[int] = Field(None, description="Capacité générale hôtel pour événements")
    pr_acces_facile: Optional[int] = Field(None, description="Capacité espaces à accès facile/PMR")
    pr_banquet: Optional[int] = Field(None, description="Capacité banquet/dîner assis")
    pr_contact: Optional[int] = Field(None, description="Capacité espaces de networking/contact")
    pr_room_nb: Optional[int] = Field(None, description="Nombre total de salles de réunion/événement")
    
    # Caractéristiques booléennes (Yes/No)
    pr_lieu_atypique: Optional[str] = Field(None, description="Lieu atypique/original (Yes/No)")
    pr_nature: Optional[str] = Field(None, description="Proximité nature/espaces verts (Yes/No)")
    pr_mer: Optional[str] = Field(None, description="Proximité mer/océan (Yes/No)")
    pr_montagne: Optional[str] = Field(None, description="Proximité montagne (Yes/No)")
    pr_centre_ville: Optional[str] = Field(None, description="Localisation centre-ville (Yes/No)")
    pr_parking: Optional[str] = Field(None, description="Parking disponible (Yes/No)")
    pr_restaurant: Optional[str] = Field(None, description="Restaurant sur place (Yes/No)")
    pr_piscine: Optional[str] = Field(None, description="Piscine disponible (Yes/No)")
    pr_spa: Optional[str] = Field(None, description="Spa/bien-être disponible (Yes/No)")
    pr_wifi: Optional[str] = Field(None, description="WiFi gratuit disponible (Yes/No)")
    pr_sun: Optional[str] = Field(None, description="Espaces ensoleillés/terrasses (Yes/No)")
    pr_contemporaine: Optional[str] = Field(None, description="Architecture contemporaine (Yes/No)")
    pr_acces_pmr: Optional[str] = Field(None, description="Accès PMR/handicapés (Yes/No)")
    pr_visio: Optional[str] = Field(None, description="Équipements visioconférence (Yes/No)")
    pr_eco_label: Optional[str] = Field(None, description="Label écologique/développement durable (Yes/No)")
    pr_rooftop: Optional[str] = Field(None, description="Rooftop/toit-terrasse disponible (Yes/No)")
    pr_esat: Optional[str] = Field(None, description="Partenariat ESAT/insertion (Yes/No)")
    
    # Résumé et métadonnées
    summary: Optional[str] = Field(None, description="Résumé descriptif de l'hôtel en français (150 mots max, optimisé SEO, focus événementiel/business)")
    
    # Nouvelles données enrichies Firecrawl
    hotel_website_title: Optional[str] = Field(None, description="Titre officiel du site web")
    hotel_phone: Optional[str] = Field(None, description="Numéro de téléphone principal")
    hotel_email: Optional[str] = Field(None, description="Email de contact principal")
    opening_hours: Optional[str] = Field(None, description="Horaires d'ouverture")
    price_range: Optional[str] = Field(None, description="Gamme de prix (€, $$, etc.)")
    
    # Données salles de réunion (nouveau!)
    meeting_rooms_available: Optional[bool] = Field(None, description="Salles de réunion disponibles")
    meeting_rooms_count: Optional[int] = Field(None, description="Nombre de salles de réunion")
    largest_room_capacity: Optional[int] = Field(None, description="Capacité de la plus grande salle")

    # Images du site
    photos_urls: Optional[List[str]] = Field(default_factory=list, description="URLs des photos de l'hôtel")
    photos_count: Optional[int] = Field(None, description="Nombre de photos collectées")


@dataclass
class FirecrawlConfig:
    """Configuration pour Firecrawl"""
    api_key: str
    batch_size: int = 10  # Batch size = rate limit (10 req/min)
    max_concurrent_batches: int = 3
    timeout: int = 120
    formats: List[str] = None
    only_main_content: bool = True
    include_tags: List[str] = None
    exclude_tags: List[str] = None
    rate_limit_requests_per_minute: int = 10  # Plan gratuit = 10 req/min
    rate_limit_wait_seconds: int = 65  # Attendre 65s après chaque batch de 10
    
    def __post_init__(self):
        if self.formats is None:
            self.formats = ['markdown', 'html']
        if self.include_tags is None:
            self.include_tags = ['p', 'h1', 'h2', 'h3', 'div', 'span', 'ul', 'li']
        if self.exclude_tags is None:
            self.exclude_tags = ['script', 'style', 'nav', 'footer', 'ads']
        
        # Ajuster batch_size au rate limit pour parallélisme optimal
        if self.batch_size > self.rate_limit_requests_per_minute:
            self.batch_size = self.rate_limit_requests_per_minute


class FirecrawlExtractor:
    """Extracteur Firecrawl avec architecture parallélisée et robuste"""
    
    def __init__(self, config: FirecrawlConfig = None):
        if FirecrawlApp is None:
            raise ImportError("Firecrawl-py requis. Installez avec: pip install firecrawl-py")
        
        # Configuration
        self.config = config or FirecrawlConfig(
            api_key=os.getenv('FIRECRAWL_API_KEY', ''),
            rate_limit_requests_per_minute=int(os.getenv('FIRECRAWL_RATE_LIMIT', '10')),
            rate_limit_wait_seconds=int(os.getenv('FIRECRAWL_WAIT_SECONDS', '65'))
        )
        
        if not self.config.api_key:
            raise ValueError("FIRECRAWL_API_KEY manquant dans .env")
        
        # Client Firecrawl
        self.app = FirecrawlApp(api_key=self.config.api_key)
        
        # Schéma d'extraction
        self.extraction_schema = HotelFirecrawlSchema.model_json_schema()
        
        # Session HTTP pour tracking des jobs
        self.session = None
        
        # Statistiques
        self.stats = {
            'total_urls': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_processing_time': 0.0,
            'start_time': None
        }
    
    async def __aenter__(self):
        """Context manager entry"""
        self.session = aiohttp.ClientSession()
        self.stats['start_time'] = datetime.now()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
        
        # Log final stats
        if self.stats['start_time']:
            total_time = (datetime.now() - self.stats['start_time']).total_seconds()
            self.stats['total_processing_time'] = total_time
            print(f"🎯 FIRECRAWL STATS FINALES:")
            print(f"   📊 Total URLs: {self.stats['total_urls']}")
            print(f"   ✅ Succès: {self.stats['successful_extractions']}")
            print(f"   ❌ Échecs: {self.stats['failed_extractions']}")
            print(f"   ⏱️ Temps total: {total_time:.1f}s")
            if self.stats['successful_extractions'] > 0:
                avg_time = total_time / self.stats['successful_extractions']
                print(f"   ⚡ Temps moyen/URL: {avg_time:.1f}s")
    
    async def extract_hotels_batch(self, hotel_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extrait les données pour un batch d'hôtels en parallèle
        
        Args:
            hotel_data: Liste de données hôtels avec 'name', 'address', 'website_url'
            
        Returns:
            Liste des résultats d'extraction
        """
        
        print(f"🚀 Début extraction Firecrawl pour {len(hotel_data)} hôtels")
        self.stats['total_urls'] = len(hotel_data)
        
        # Diviser en batches selon rate limit (10 req/min = 10 URLs par batch)
        batches = self._create_batches(hotel_data, self.config.rate_limit_requests_per_minute)
        print(f"📦 {len(batches)} batches créés (taille: {self.config.rate_limit_requests_per_minute} = rate limit)")
        
        # 🚦 TRAITEMENT SÉQUENTIEL des batches avec attente entre eux
        print(f"🚦 Mode séquentiel avec {self.config.rate_limit_wait_seconds}s d'attente entre batches")
        
        all_batch_results = []
        for i, batch in enumerate(batches):
            if i > 0:  # Attendre après chaque batch sauf le premier
                print(f"⏳ Attente {self.config.rate_limit_wait_seconds}s avant batch {i+1}")
                await asyncio.sleep(self.config.rate_limit_wait_seconds)
            
            # Traiter le batch en parallèle interne (jusqu'à 10 requêtes simultanées)
            batch_result = await self._process_batch_parallel(batch, i+1)
            all_batch_results.append(batch_result)
        
        # Consolider tous les résultats
        all_results = []
        for batch_result in all_batch_results:
            if isinstance(batch_result, Exception):
                print(f"❌ Erreur batch: {batch_result}")
                continue
            all_results.extend(batch_result)
        
        return all_results
    
    async def _process_batch_parallel(self, hotel_batch: List[Dict], batch_num: int) -> List[Dict]:
        """Traite un batch d'hôtels"""
        
        print(f"📦 Traitement batch {batch_num} ({len(hotel_batch)} hôtels) - PARALLÈLE interne")
        
        # Préparer les URLs pour Firecrawl
        urls = []
        url_to_hotel = {}
        
        for hotel in hotel_batch:
            website_url = hotel.get('website_url', '')
            if website_url and website_url.startswith(('http://', 'https://')):
                urls.append(website_url)
                url_to_hotel[website_url] = hotel
        
        if not urls:
            print(f"⚠️ Batch {batch_num}: Aucune URL valide trouvée")
            return []
        
        try:
            # Appel Firecrawl batch scrape avec extraction
            batch_result = await self._firecrawl_batch_scrape(urls, batch_num)
            
            # Traiter les résultats
            processed_results = []
            if batch_result and hasattr(batch_result, 'items'):
                for url, result in batch_result.items():
                    hotel_info = url_to_hotel.get(url, {})
                    processed_result = self._process_extraction_result(
                        hotel_info, url, result, batch_num
                    )
                    processed_results.append(processed_result)
            else:
                raise Exception("batch_result invalide - pas d'attribut items")
            
            success_count = sum(1 for r in processed_results if r['success'])
            print(f"✅ Batch {batch_num}: {success_count}/{len(processed_results)} succès")
            
            return processed_results
            
        except Exception as e:
            print(f"❌ Erreur batch {batch_num}: {e}")
            # Retourner des résultats d'échec pour chaque hôtel
            return [self._create_failure_result(hotel, str(e)) for hotel in hotel_batch]
    
    async def _firecrawl_batch_scrape(self, urls: List[str], batch_num: int) -> Dict[str, Any]:
        """Effectue le scraping Firecrawl en mode batch"""
        
        print(f"🔥 Firecrawl batch {batch_num}: scraping {len(urls)} URLs...")
        
        try:
            # 🔧 CORRECTION: Configuration simplifiée pour API v1
            # Plus besoin de configurations séparées avec la nouvelle API
            
            # Appel API Firecrawl
            # Note: Firecrawl batch peut prendre du temps, nous simulons ici
            results = {}
            
            # En mode réel, utiliser:
            # batch_job = self.app.batch_scrape_urls(urls, scrape_config)
            # results = self.app.get_batch_job_status(batch_job['jobId'])
            
            # 🔧 Utilisation de la méthode extract() pour extraction structurée
            print(f"🔧 Extraction structurée pour {len(urls)} URLs...")
            
            # 🚦 TRAITEMENT SÉQUENTIEL: Rate limiting pour plan gratuit Firecrawl
            print(f"🚦 Traitement séquentiel de {len(urls)} URLs avec rate limiting...")
            
            async def scrape_single_url(url):
                """Scrape une seule URL de manière asynchrone (rate limiting géré au niveau supérieur)"""
                try:
                    # Rate limiting géré dans la boucle séquentielle principale
                    
                    print(f"🎯 Extraction structurée URL: {url}")
                    # 🎯 EXTRACTION STRUCTURÉE avec syntaxe correcte Firecrawl
                    loop = asyncio.get_event_loop()
                    # ✅ VRAIE SYNTAXE FIRECRAWL 2025 selon documentation officielle
                    result = await loop.run_in_executor(
                        None, 
                        lambda: self.app.extract(
                            urls=[url],  # Liste d'URLs (obligatoire)
                            prompt=self._build_extraction_prompt(),
                            schema=self.extraction_schema
                        )
                    )
                    
                    if result is None:
                        print(f"🔴 RÉSULTAT NULL détecté pour {url}")
                        return url, {'error': 'Extract result is None'}
                    
                    # 🎯 TRAITEMENT RÉSULTAT EXTRACT - ExtractResponse de Firecrawl
                    if hasattr(result, '__dict__'):
                        result_dict = result.__dict__
                        print(f"✅ Extraction structurée réussie pour {url}")
                        print(f"🔧 ExtractResponse keys: {list(result_dict.keys())}")
                    elif isinstance(result, list) and len(result) > 0:
                        extract_data = result[0]
                        result_dict = extract_data.__dict__ if hasattr(extract_data, '__dict__') else extract_data
                        print(f"✅ Extraction structurée réussie pour {url}")
                    elif isinstance(result, dict):
                        result_dict = result
                        print(f"✅ Extraction structurée réussie pour {url}")
                    else:
                        print(f"⚠️ Format inattendu pour {url}: {type(result)}")
                        result_dict = {'error': f'Format inattendu: {type(result)}', 'raw_data': str(result)}

                    # Récupérer aussi des images du site
                    images = await self._scrape_images(url)
                    result_dict['photos_urls'] = images
                    result_dict['photos_count'] = len(images)
                    if isinstance(result_dict.get('data'), dict):
                        result_dict['data']['photos_urls'] = images
                        result_dict['data']['photos_count'] = len(images)

                    return url, result_dict
                    
                except Exception as e:
                    print(f"❌ Erreur scraping {url}: {e}")
                    return url, {'error': str(e)}
            
            # 🚀 PARALLÉLISME RESTAURÉ dans le batch (max 10 URLs simultanées)
            print(f"🚀 Traitement parallèle de {len(urls)} URLs (batch rate-limited)")
            
            scrape_tasks = [scrape_single_url(url) for url in urls]
            parallel_results = await asyncio.gather(*scrape_tasks, return_exceptions=True)
            
            # Assembler les résultats
            results = {}
            for result in parallel_results:
                if isinstance(result, Exception):
                    print(f"❌ Exception dans task parallèle: {result}")
                    continue
                    
                url, data = result
                results[url] = data
            
            print(f"🚀 Parallélisme terminé: {len(results)}/{len(urls)} URLs traitées")
            return results
                
        except Exception as batch_error:
            print(f"❌ Échec extraction batch: {batch_error}")
            # Retourner des résultats d'erreur pour toutes les URLs
            for url in urls:
                results[url] = {'error': f'Extraction batch échouée: {batch_error}'}
            return results
            
        except Exception as e:
            print(f"❌ Erreur Firecrawl batch {batch_num}: {e}")
            raise
    
    async def _extract_single_url_structured(self, url: str) -> Dict[str, Any]:
        """Extrait une seule URL avec la méthode extract structurée
        
        Args:
            url (str): URL à extraire
            
        Returns:
            Dict[str, Any]: Résultat d'extraction 
        """
        
        try:
            # Rate limiting respectueux pour Firecrawl
            await asyncio.sleep(0.5)  # 500ms entre chaque URL
            
            # Utiliser extract() pour extraction structurée
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.app.extract(
                    urls=[url],  # Liste d'URLs (obligatoire)
                    prompt=self._build_extraction_prompt(),
                    schema=self.extraction_schema
                )
            )
            
            if hasattr(result, '__dict__'):
                result_dict = result.__dict__
                print(f"   ✅ Extraction structurée réussie")
                return result_dict
            else:
                return result if isinstance(result, dict) else {'data': result}
                
        except Exception as e:
            print(f"❌ Erreur extraction {url}: {e}")
            return {'error': f'Extraction échouée: {e}'}

    async def _scrape_images(self, url: str, max_images: int = 15) -> List[str]:
        """Scrape la page pour récupérer des URLs d'images pertinentes"""

        loop = asyncio.get_event_loop()
        try:
            scrape_result = await loop.run_in_executor(
                None,
                lambda: self.app.scrape_url(url, formats=['html'])
            )

            html = ''
            if hasattr(scrape_result, 'html') and scrape_result.html:
                html = scrape_result.html
            elif isinstance(scrape_result, dict):
                html = scrape_result.get('html') or scrape_result.get('rawHtml', '')

            if not html:
                return []

            from bs4 import BeautifulSoup
            from urllib.parse import urljoin

            soup = BeautifulSoup(html, 'html.parser')
            images = []
            for img in soup.find_all('img'):
                src = img.get('src')
                alt = (img.get('alt') or '').lower()
                if not src:
                    continue
                if any(keyword in src.lower() for keyword in ['logo', 'icon']) or 'logo' in alt or 'icon' in alt:
                    continue
                if src.startswith('//'):
                    src = 'https:' + src
                if src.startswith('/'):
                    src = urljoin(url, src)
                if src.startswith('data:'):
                    continue
                if src not in images:
                    images.append(src)
                if len(images) >= max_images:
                    break

            return images

        except Exception as e:
            print(f"❌ Erreur récupération images {url}: {e}")
            return []
    
    def _build_extraction_prompt(self) -> str:
        """Construit le prompt d'extraction optimisé pour les hôtels"""
        
        return """Tu es un expert en analyse de sites web d'hôtels et centres de congrès.

MISSION: Extraire les informations précises sur cet établissement selon le schéma JSON fourni.

PRIORITÉS D'EXTRACTION:
1. Capacités et nombre de chambres (chercher dans sections "Hébergement", "Rooms", "Chambres")
2. Équipements et services (parking, restaurant, piscine, spa, wifi, etc.)
3. Informations sur les salles de réunion/événements ("Meeting rooms", "Espaces événementiels", "Séminaires")
4. Localisation et caractéristiques (centre-ville, nature, mer, montagne)
5. Données de contact (téléphone, email, horaires)

INSTRUCTIONS SPÉCIALES:
- Pour les champs Yes/No: utilise exactement "Yes" ou "No" (jamais "Oui"/"Non")
- Pour les nombres: extrait uniquement les chiffres (ex: "150 chambres" → 150)
- Pour le résumé: maximum 150 mots, focus sur capacités événementielles
- Ignore les données de navigation, cookies, menus

Si une information n'est pas trouvée, laisse le champ à null."""
    
    def _process_extraction_result(self, hotel_info: Dict, url: str, firecrawl_result: Dict, batch_num: int) -> Dict[str, Any]:
        """Traite le résultat d'extraction Firecrawl pour un hôtel"""
        
        hotel_name = hotel_info.get('name', 'Hotel_Unknown')
        
        try:
            # Vérification basique du résultat
            if firecrawl_result is None:
                raise Exception("firecrawl_result est None")
            
            # Vérifier si l'extraction a réussi
            if 'error' in firecrawl_result and firecrawl_result['error'] is not None:
                raise Exception(f"Erreur Firecrawl: {firecrawl_result['error']}")
            
            # 🎯 TRAITEMENT RÉSULTAT EXTRACT() - Nouveau format structuré
            extracted_data = None
            content_length = 0
            
            # Cas 1: ExtractResponse converti en dict
            if isinstance(firecrawl_result, dict) and 'data' in firecrawl_result:
                print(f"🎯 ExtractResponse détecté pour {hotel_name}")
                # Les données structurées sont dans le champ 'data'
                raw_extract_data = firecrawl_result.get('data', {})
                if isinstance(raw_extract_data, list) and len(raw_extract_data) > 0:
                    extracted_data = raw_extract_data[0]  # Premier élément de la liste
                else:
                    extracted_data = raw_extract_data
                content_length = 0
                print(f"🔧 Données extraites type: {type(extracted_data)}")
                
            # Cas 2: Données extract() directes 
            elif isinstance(firecrawl_result, dict) and any(key in ['capacite_max', 'nombre_chambre', 'summary', 'pr_parking'] for key in firecrawl_result.keys()):
                print(f"🎯 Données extract() directes détectées pour {hotel_name}")
                extracted_data = firecrawl_result
                content_length = 0
                
            # Cas 3: Erreur dans extract()
            elif isinstance(firecrawl_result, dict) and 'error' in firecrawl_result:
                raise Exception(f"Erreur Firecrawl extract: {firecrawl_result['error']}")
            
            # Cas 4: Résultat vide ou format inattendu
            elif isinstance(firecrawl_result, dict) and len(firecrawl_result) == 0:
                print(f"⚠️ Résultat vide pour {hotel_name}")
                extracted_data = {}
            
            else:
                raise Exception(f"Format de réponse Firecrawl inattendu: {type(firecrawl_result)} - Keys: {list(firecrawl_result.keys()) if isinstance(firecrawl_result, dict) else 'N/A'}")
            
            # Validation et nettoyage des données extraites
            validated_data = self._validate_extracted_data(extracted_data) if extracted_data else {}
            
            # Métadonnées enrichies
            metadata = {
                'url': url,
                'title': firecrawl_result.get('title', '') if isinstance(firecrawl_result, dict) else '',
                'description': firecrawl_result.get('description', '') if isinstance(firecrawl_result, dict) else '',
                'content_length': content_length,
                'extraction_method': 'firecrawl_extract',
                'batch_number': batch_num,
                'processing_date': datetime.now().isoformat(),
                'firecrawl_metadata': firecrawl_result.get('metadata', {}) if isinstance(firecrawl_result, dict) else {}
            }
            
            # Résultat de succès
            result = {
                'success': True,
                'hotel_name': hotel_name,
                'hotel_address': hotel_info.get('address', ''),
                'website_data': validated_data,
                'metadata': metadata,
                'error': None
            }
            
            self.stats['successful_extractions'] += 1
            
            # Log pour extraction réussie
            if validated_data and isinstance(validated_data, dict):
                try:
                    fields_count = len([k for k, v in validated_data.items() if v is not None])
                    print(f"✅ {hotel_name}: extraction structurée réussie ({fields_count} champs)")
                except Exception as items_error:
                    print(f"✅ {hotel_name}: extraction structurée réussie")
            else:
                print(f"✅ {hotel_name}: extraction réussie")
            
            return result
            
        except Exception as e:
            self.stats['failed_extractions'] += 1
            print(f"❌ {hotel_name}: {e}")
            return self._create_failure_result(hotel_info, str(e))
    
    
    def _validate_extracted_data(self, raw_data: Dict) -> Dict[str, Any]:
        """Valide et nettoie les données extraites par Firecrawl"""
        
        if not raw_data or not isinstance(raw_data, dict):
            return {}
        
        validated = {}
        
        # Valider les champs numériques
        numeric_fields = [
            'capacite_max', 'nombre_chambre', 'nombre_chambre_twin', 'nombre_etoile',
            'pr_amphi', 'pr_hotel', 'pr_acces_facile', 'pr_banquet', 
            'pr_contact', 'pr_room_nb', 'meeting_rooms_count', 'largest_room_capacity'
        ]
        
        for field in numeric_fields:
            value = raw_data.get(field)
            if value is not None:
                try:
                    validated[field] = int(value) if str(value).isdigit() else None
                except (ValueError, TypeError):
                    validated[field] = None
            else:
                validated[field] = None
        
        # Valider les champs Yes/No
        yes_no_fields = [
            'pr_lieu_atypique', 'pr_nature', 'pr_mer', 'pr_montagne', 'pr_centre_ville',
            'pr_parking', 'pr_restaurant', 'pr_piscine', 'pr_spa', 'pr_wifi', 'pr_sun',
            'pr_contemporaine', 'pr_acces_pmr', 'pr_visio', 'pr_eco_label', 'pr_rooftop', 'pr_esat'
        ]
        
        for field in yes_no_fields:
            value = raw_data.get(field)
            if value and isinstance(value, str):
                cleaned_value = value.strip().lower()
                if cleaned_value in ['yes', 'oui', 'true', '1', 'disponible', 'présent']:
                    validated[field] = 'Yes'
                elif cleaned_value in ['no', 'non', 'false', '0', 'indisponible', 'absent']:
                    validated[field] = 'No'
                else:
                    validated[field] = None
            else:
                validated[field] = None
        
        # Valider les champs booléens
        boolean_fields = ['meeting_rooms_available']
        for field in boolean_fields:
            value = raw_data.get(field)
            if value is not None:
                validated[field] = bool(value)
            else:
                validated[field] = None
        
        # Valider les champs texte
        text_fields = [
            'summary', 'hotel_website_title', 'hotel_phone', 'hotel_email', 
            'opening_hours', 'price_range'
        ]
        
        for field in text_fields:
            value = raw_data.get(field)
            if value and isinstance(value, str):
                cleaned_value = value.strip()
                if field == 'summary' and len(cleaned_value) > 500:
                    # Limiter le résumé
                    words = cleaned_value.split()[:150]
                    validated[field] = ' '.join(words) + ('...' if len(words) == 150 else '')
                else:
                    validated[field] = cleaned_value
            else:
                validated[field] = None if field != 'summary' else ""

        # Images
        photos = raw_data.get('photos_urls', [])
        if isinstance(photos, list):
            validated['photos_urls'] = photos[:15]
            validated['photos_count'] = len(photos[:15])
        else:
            validated['photos_urls'] = []
            validated['photos_count'] = raw_data.get('photos_count', 0) or 0

        return validated
    
    def _create_failure_result(self, hotel_info: Dict, error_message: str) -> Dict[str, Any]:
        """Crée un résultat d'échec standardisé"""
        
        return {
            'success': False,
            'hotel_name': hotel_info.get('name', 'Hotel_Unknown'),
            'hotel_address': hotel_info.get('address', ''),
            'website_data': {
                'photos_urls': [],
                'photos_count': 0
            },
            'metadata': {
                'url': hotel_info.get('website_url', ''),
                'extraction_method': 'firecrawl',
                'processing_date': datetime.now().isoformat()
            },
            'error': error_message
        }
    
    def _create_batches(self, data: List[Any], batch_size: int) -> List[List[Any]]:
        """Divise les données en batches"""
        return [data[i:i + batch_size] for i in range(0, len(data), batch_size)]


# Fonction d'usage pratique
async def extract_hotels_with_firecrawl(hotels_data: List[Dict[str, Any]], 
                                       output_dir: str = "outputs") -> List[Dict[str, Any]]:
    """Fonction utilitaire pour extraire des données d'hôtels avec Firecrawl
    
    Args:
        hotels_data: Liste de dictionnaires avec name, address, website_url
        output_dir: Dossier de sortie pour logs
        
    Returns:
        Liste des résultats d'extraction
    """
    
    config = FirecrawlConfig(
        api_key=os.getenv('FIRECRAWL_API_KEY', ''),
        rate_limit_requests_per_minute=int(os.getenv('FIRECRAWL_RATE_LIMIT', '10')),
        rate_limit_wait_seconds=int(os.getenv('FIRECRAWL_WAIT_SECONDS', '65'))
    )
    
    async with FirecrawlExtractor(config) as extractor:
        results = await extractor.extract_hotels_batch(hotels_data)
    
    # Sauvegarder les résultats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = Path(output_dir) / f"firecrawl_extraction_{timestamp}.json"
    
    Path(output_dir).mkdir(exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"📄 Résultats sauvegardés: {output_file}")
    
    return results