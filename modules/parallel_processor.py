"""
Module de traitement parallèle pour les extractions hôtelières
Gère la parallélisation avec 4 workers pour optimiser les performances
"""

import asyncio
import time
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime

from .cvent_extractor import extract_cvent_data
from .gmaps_extractor import extract_hotels_batch
from .website_extractor import extract_hotels_websites_batch


@dataclass
class ParallelConfig:
    """Configuration du traitement parallèle"""
    max_workers: int = 4
    batch_size: int = 10  # Nombre d'hôtels par batch
    cvent_timeout: int = 45  # Timeout par hôtel Cvent
    gmaps_timeout: int = 30   # Timeout par hôtel Google Maps
    website_timeout: int = 60  # Timeout par hôtel Website (plus long car scraping + LLM)
    
    @classmethod
    def from_machine_specs(cls, ram_gb: int = 16, cvent_only: bool = False):
        """Crée une configuration adaptée aux specs de la machine
        
        Args:
            ram_gb (int): RAM disponible en GB
            cvent_only (bool): Si true, optimise pour Cvent uniquement (plus conservateur)
            
        Returns:
            ParallelConfig: Configuration optimisée
        """
        if cvent_only:
            # Configuration plus conservatrice pour Playwright/Cvent
            if ram_gb >= 16:
                return cls(max_workers=2, batch_size=3, cvent_timeout=60)  # Plus conservateur
            elif ram_gb >= 8:
                return cls(max_workers=1, batch_size=2, cvent_timeout=60)
            else:
                return cls(max_workers=1, batch_size=1, cvent_timeout=60)
        else:
            # Configuration normale pour Google Maps + Cvent
            if ram_gb >= 16:
                return cls(max_workers=4, batch_size=8)
            elif ram_gb >= 8:
                return cls(max_workers=2, batch_size=5)
            else:
                return cls(max_workers=1, batch_size=3)


class ProgressReporter:
    """Reporter de progression thread-safe"""
    
    def __init__(self, total_hotels: int):
        self.total_hotels = total_hotels
        self.completed = 0
        self.cvent_completed = 0
        self.gmaps_completed = 0
        self.website_completed = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
    
    def update_cvent_progress(self, success: bool = True):
        """Met à jour la progression Cvent"""
        with self.lock:
            self.cvent_completed += 1
            if not success:
                self.errors += 1
    
    def update_gmaps_progress(self, success: bool = True):
        """Met à jour la progression Google Maps"""
        with self.lock:
            self.gmaps_completed += 1
            if not success:
                self.errors += 1
    
    def update_website_progress(self, success: bool = True):
        """Met à jour la progression Website"""
        with self.lock:
            self.website_completed += 1
            if not success:
                self.errors += 1
    
    def update_completed(self):
        """Met à jour le compteur global de completion"""
        with self.lock:
            self.completed += 1
    
    def get_progress_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de progression"""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            
            return {
                'total_hotels': self.total_hotels,
                'completed': self.completed,
                'cvent_completed': self.cvent_completed,
                'gmaps_completed': self.gmaps_completed,
                'website_completed': self.website_completed,
                'errors': self.errors,
                'progress_percent': (self.completed / self.total_hotels * 100) if self.total_hotels > 0 else 0,
                'elapsed_time': elapsed_time,
                'eta_seconds': (elapsed_time / self.completed * (self.total_hotels - self.completed)) if self.completed > 0 else 0
            }


class ParallelHotelProcessor:
    """Processeur parallèle pour l'extraction d'informations hôtelières"""
    
    def __init__(self, config: ParallelConfig = None):
        self.config = config or ParallelConfig.from_machine_specs()
        self.progress_reporter = None
        self._running = False
    
    async def process_hotels_parallel(
        self, 
        hotels_data: List[Dict[str, str]], 
        extract_cvent: bool = True,
        extract_gmaps: bool = True,
        extract_website: bool = False,
        progress_callback: Optional[Callable] = None
    ) -> List[Dict[str, Any]]:
        """Traite une liste d'hôtels en parallèle
        
        Args:
            hotels_data (List[Dict[str, str]]): Données des hôtels (name, address, url)
            extract_cvent (bool): Activer l'extraction Cvent
            extract_gmaps (bool): Activer l'extraction Google Maps
            extract_website (bool): Activer l'extraction Website
            progress_callback (Optional[Callable]): Callback de progression
            
        Returns:
            List[Dict[str, Any]]: Résultats consolidés
        """
        
        print(f"🚀 Démarrage traitement parallèle: {len(hotels_data)} hôtels, {self.config.max_workers} workers")
        
        self.progress_reporter = ProgressReporter(len(hotels_data))
        self._running = True
        
        # Diviser en batches
        batches = self._create_batches(hotels_data)
        
        # Créer les tâches parallèles avec limitation pour Firecrawl
        tasks = []
        # 🚦 FIRECRAWL RATE LIMITING: 1 seul batch à la fois si website extraction
        max_concurrent = 1 if extract_website else self.config.max_workers
        semaphore = asyncio.Semaphore(max_concurrent)
        
        if extract_website:
            print(f"🚦 Limitation à 1 batch concurrent (Firecrawl rate limiting)")
        else:
            print(f"🚀 {self.config.max_workers} batches concurrents (pas de website)")
        
        for batch_index, batch in enumerate(batches):
            task = self._process_batch_with_semaphore(
                semaphore, 
                batch, 
                batch_index,
                extract_cvent, 
                extract_gmaps,
                extract_website,
                progress_callback
            )
            tasks.append(task)
        
        # Exécuter les batches (séquentiel si website activé via semaphore)
        try:
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Consolider les résultats
            consolidated_results = []
            for batch_result in batch_results:
                if isinstance(batch_result, Exception):
                    print(f"❌ Erreur batch: {batch_result}")
                    continue
                
                consolidated_results.extend(batch_result)
            
            self._running = False
            
            # Forcer une mise à jour finale après tous les batches
            if progress_callback:
                final_stats = self.progress_reporter.get_progress_stats()
                await self._safe_callback(progress_callback, final_stats)
            
            # Statistiques finales
            final_stats = self.progress_reporter.get_progress_stats()
            print(f"✅ Traitement terminé: {final_stats['completed']}/{final_stats['total_hotels']} hôtels")
            print(f"⏱️ Temps total: {final_stats['elapsed_time']:.1f}s")
            print(f"❌ Erreurs: {final_stats['errors']}")
            
            return consolidated_results
        
        except Exception as e:
            self._running = False
            print(f"❌ Erreur critique traitement parallèle: {e}")
            raise
    
    def _create_batches(self, hotels_data: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
        """Divise les hôtels en batches pour le traitement parallèle
        
        Args:
            hotels_data (List[Dict[str, str]]): Liste des hôtels
            
        Returns:
            List[List[Dict[str, str]]]: Batches d'hôtels
        """
        
        batches = []
        for i in range(0, len(hotels_data), self.config.batch_size):
            batch = hotels_data[i:i + self.config.batch_size]
            batches.append(batch)
        
        print(f"📦 Création de {len(batches)} batches de ~{self.config.batch_size} hôtels")
        return batches
    
    async def _process_batch_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        batch: List[Dict[str, str]],
        batch_index: int,
        extract_cvent: bool,
        extract_gmaps: bool,
        extract_website: bool,
        progress_callback: Optional[Callable]
    ) -> List[Dict[str, Any]]:
        """Traite un batch avec limitation de concurrence ET parallélisation intra-batch
        
        Args:
            semaphore (asyncio.Semaphore): Semaphore pour limiter la concurrence
            batch (List[Dict[str, str]]): Batch d'hôtels à traiter
            batch_index (int): Index du batch
            extract_cvent (bool): Activer Cvent
            extract_gmaps (bool): Activer Google Maps  
            extract_website (bool): Activer Website
            progress_callback (Optional[Callable]): Callback de progression
            
        Returns:
            List[Dict[str, Any]): Résultats du batch
        """
        
        async with semaphore:
            start_time = time.time()
            print(f"🔄 Worker {batch_index + 1}: Démarrage traitement de {len(batch)} hôtels...")
            
            # 🚀 NOUVELLE APPROCHE: Parallélisation des hôtels DANS le batch
            if len(batch) > 1 and (extract_gmaps or extract_website):
                print(f"   🚀 Mode parallèle intra-batch: {len(batch)} hôtels simultanés")
                
                # Créer les tâches parallèles pour chaque hôtel
                hotel_tasks = []
                for hotel_data in batch:
                    task = self._process_single_hotel_tracked(
                        hotel_data, extract_cvent, extract_gmaps, extract_website, progress_callback
                    )
                    hotel_tasks.append(task)
                
                # Exécuter tous les hôtels en parallèle
                batch_results = await asyncio.gather(*hotel_tasks, return_exceptions=True)
                
                # Gérer les exceptions
                processed_results = []
                for i, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        error_result = self._create_error_result(batch[i], str(result))
                        processed_results.append(error_result)
                        self.progress_reporter.update_completed()
                        self.progress_reporter.errors += 1
                    else:
                        processed_results.append(result)
                        self.progress_reporter.update_completed()
                
                batch_results = processed_results
                
                # Callback de progression après traitement parallèle intra-batch
                if progress_callback:
                    stats = self.progress_reporter.get_progress_stats()
                    await self._safe_callback(progress_callback, stats)
            else:
                # Mode séquentiel pour petits batches ou extraction simple
                print(f"   🔄 Mode séquentiel: {len(batch)} hôtel(s)")
                batch_results = []
                
                for i, hotel_data in enumerate(batch):
                    hotel_start = time.time()
                    try:
                        hotel_result = await self._process_single_hotel(
                            hotel_data, extract_cvent, extract_gmaps, extract_website
                        )
                        
                        batch_results.append(hotel_result)
                        self.progress_reporter.update_completed()
                        
                        hotel_elapsed = time.time() - hotel_start
                        print(f"   ✅ Hôtel {i+1}/{len(batch)} ({hotel_data.get('name', 'Unknown')}) traité en {hotel_elapsed:.1f}s")
                        
                        # Callback de progression
                        if progress_callback:
                            stats = self.progress_reporter.get_progress_stats()
                            await self._safe_callback(progress_callback, stats)
                    
                    except Exception as e:
                        print(f"❌ Erreur hôtel {hotel_data.get('name', 'Unknown')}: {e}")
                        
                        error_result = self._create_error_result(hotel_data, str(e))
                        batch_results.append(error_result)
                        
                        self.progress_reporter.update_completed()
                        self.progress_reporter.errors += 1
            
            batch_elapsed = time.time() - start_time
            print(f"✅ Worker {batch_index + 1}: Batch terminé en {batch_elapsed:.1f}s ({len(batch_results)} résultats)")
            print(f"   📊 Moyenne: {batch_elapsed/len(batch):.1f}s par hôtel")
            return batch_results
    
    async def _process_single_hotel_tracked(
        self, 
        hotel_data: Dict[str, str], 
        extract_cvent: bool, 
        extract_gmaps: bool,
        extract_website: bool,
        progress_callback: Optional[Callable]
    ) -> Dict[str, Any]:
        """Version trackée de _process_single_hotel pour parallélisation intra-batch"""
        
        hotel_start = time.time()
        try:
            hotel_result = await self._process_single_hotel(
                hotel_data, extract_cvent, extract_gmaps, extract_website
            )
            
            hotel_elapsed = time.time() - hotel_start
            print(f"   ✅ Hôtel ({hotel_data.get('name', 'Unknown')}) traité en {hotel_elapsed:.1f}s")
            
            # Callback de progression
            if progress_callback:
                stats = self.progress_reporter.get_progress_stats()
                await self._safe_callback(progress_callback, stats)
            
            return hotel_result
            
        except Exception as e:
            print(f"❌ Erreur hôtel {hotel_data.get('name', 'Unknown')}: {e}")
            raise
    
    async def _process_single_hotel(
        self, 
        hotel_data: Dict[str, str], 
        extract_cvent: bool, 
        extract_gmaps: bool,
        extract_website: bool
    ) -> Dict[str, Any]:
        """Traite un seul hôtel avec extractions parallèles
        
        Args:
            hotel_data (Dict[str, str]): Données de l'hôtel
            extract_cvent (bool): Activer Cvent
            extract_gmaps (bool): Activer Google Maps
            extract_website (bool): Activer Website
            
        Returns:
            Dict[str, Any]: Résultat consolidé pour l'hôtel
        """
        
        # Phase 1: Extractions parallèles (Cvent + GMaps)
        phase1_tasks = {}
        
        if extract_cvent:
            phase1_tasks['cvent'] = self._extract_cvent_async(hotel_data)
        
        if extract_gmaps:
            phase1_tasks['gmaps'] = self._extract_gmaps_async(hotel_data)
        
        # Exécuter les extractions Phase 1 en parallèle
        results = {}
        
        if phase1_tasks:
            completed_tasks = await asyncio.gather(
                *phase1_tasks.values(), 
                return_exceptions=True
            )
            
            # Mapper les résultats Phase 1
            for i, (task_name, _) in enumerate(phase1_tasks.items()):
                task_result = completed_tasks[i]
                
                if isinstance(task_result, Exception):
                    print(f"⚠️ Erreur {task_name} pour {hotel_data.get('name')}: {task_result}")
                    results[task_name] = None
                else:
                    results[task_name] = task_result
        
        # Phase 2: Extraction Website (dépend de GMaps)
        if extract_website:
            # Préparer les données pour l'extraction website
            website_data = {
                'name': hotel_data.get('name', ''),
                'address': hotel_data.get('address', ''),
                'gmaps_website': None
            }
            
            # Option 1: Récupérer le site web depuis GMaps extraction si disponible
            if results.get('gmaps') and results['gmaps'].get('extraction_status') == 'success':
                gmaps_result = results['gmaps']
                gmaps_website = gmaps_result.get('website', '')
                if gmaps_website and gmaps_website.strip():
                    website_data['gmaps_website'] = gmaps_website.strip()
                    print(f"🗺️ Site Google Maps trouvé pour {hotel_data.get('name')}: {gmaps_website}")
                else:
                    print(f"⚠️ Google Maps sans site web valide pour {hotel_data.get('name')}")
            
            # Option 2: Utiliser directement gmaps_website fourni dans hotel_data (NOUVEAU)
            elif hotel_data.get('gmaps_website'):
                website_data['gmaps_website'] = hotel_data['gmaps_website'].strip()
                print(f"🗺️ Site Google Maps fourni directement pour {hotel_data.get('name')}: {hotel_data['gmaps_website']}")
            
            try:
                website_result = await self._extract_website_async(website_data)
                results['website'] = website_result
            except Exception as e:
                print(f"⚠️ Erreur website pour {hotel_data.get('name')}: {e}")
                results['website'] = None
        
        # Consolider les résultats
        return self._consolidate_hotel_results(hotel_data, results)
    
    async def _extract_cvent_async(self, hotel_data: Dict[str, str]) -> Dict[str, Any]:
        """Extraction Cvent asynchrone
        
        Args:
            hotel_data (Dict[str, str]): Données de l'hôtel
            
        Returns:
            Dict[str, Any]: Résultat Cvent
        """
        
        # Exécuter l'extraction Cvent dans un thread séparé
        loop = asyncio.get_event_loop()
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = loop.run_in_executor(
                executor,
                extract_cvent_data,
                hotel_data['name'],
                hotel_data.get('address', ''),
                hotel_data['url']
            )
            
            try:
                result = await asyncio.wait_for(future, timeout=self.config.cvent_timeout)
                self.progress_reporter.update_cvent_progress(result.get('success', False))
                return result
            
            except asyncio.TimeoutError:
                self.progress_reporter.update_cvent_progress(False)
                raise Exception(f"Timeout Cvent ({self.config.cvent_timeout}s)")
    
    async def _extract_gmaps_async(self, hotel_data: Dict[str, str]) -> Dict[str, Any]:
        """Extraction Google Maps asynchrone
        
        Args:
            hotel_data (Dict[str, str]): Données de l'hôtel
            
        Returns:
            Dict[str, Any]: Résultat Google Maps
        """
        
        from .gmaps_extractor import GoogleMapsExtractor
        
        try:
            extractor = GoogleMapsExtractor()
            result = await asyncio.wait_for(
                extractor.extract_hotel_info(
                    hotel_data['name'],
                    hotel_data.get('address', '')
                ),
                timeout=self.config.gmaps_timeout
            )
            
            self.progress_reporter.update_gmaps_progress(
                result.get('extraction_status') == 'success'
            )
            return result
        
        except asyncio.TimeoutError:
            self.progress_reporter.update_gmaps_progress(False)
            raise Exception(f"Timeout Google Maps ({self.config.gmaps_timeout}s)")
    
    async def _extract_website_async(self, website_data: Dict[str, str]) -> Dict[str, Any]:
        """Extraction Website asynchrone
        
        Args:
            website_data (Dict[str, str]): Données pour l'extraction website
            
        Returns:
            Dict[str, Any]: Résultat Website
        """
        
        from .website_extractor import WebsiteExtractor
        
        try:
            async with WebsiteExtractor() as extractor:
                result = await asyncio.wait_for(
                    extractor.extract_hotel_website_data(
                        website_data['name'],
                        website_data.get('address', ''),
                        website_data.get('gmaps_website')
                    ),
                    timeout=self.config.website_timeout
                )
            
            self.progress_reporter.update_website_progress(result.get('success', False))
            return result
        
        except asyncio.TimeoutError:
            self.progress_reporter.update_website_progress(False)
            raise Exception(f"Timeout Website extraction ({self.config.website_timeout}s)")
    
    def _consolidate_hotel_results(
        self, 
        hotel_data: Dict[str, str], 
        results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Consolide les résultats de toutes les extractions pour un hôtel
        
        Args:
            hotel_data (Dict[str, str]): Données originales de l'hôtel
            results (Dict[str, Any]): Résultats des extractions
            
        Returns:
            Dict[str, Any]: Résultat consolidé au format attendu par consolidate_hotel_extractions
        """
        
        consolidated = {
            'name': hotel_data['name'],
            'address': hotel_data.get('address', ''),
            'url': hotel_data.get('url', ''),
            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'success': False,
            'cvent_data': None,
            'gmaps_data': None,
            'website_data': None,
            'errors': []
        }
        
        # Traiter les résultats Cvent avec le MÊME formatage que le mode séquentiel
        cvent_result = results.get('cvent')
        if cvent_result and cvent_result is not None:
            if cvent_result.get('success', False):
                # Appliquer le MÊME formatage que _format_extraction_result
                consolidated['cvent_data'] = {
                    'salles_count': cvent_result['data']['salles_count'],
                    'interface_type': cvent_result['data']['interface_type'],
                    'data_file': cvent_result['data']['csv_file'],  # csv_file -> data_file
                    'headers': cvent_result['data']['headers'],
                    'sample_data': cvent_result['data']['rows'][:3] if cvent_result['data']['rows'] else []  # rows -> sample_data
                }
                consolidated['success'] = True
            else:
                consolidated['errors'].append(f"Cvent: {cvent_result.get('error', 'Erreur inconnue')}")
        
        # Traiter les résultats Google Maps
        gmaps_result = results.get('gmaps')
        if gmaps_result and gmaps_result is not None:
            if gmaps_result.get('extraction_status') == 'success':
                consolidated['gmaps_data'] = gmaps_result
                consolidated['success'] = True
            else:
                consolidated['errors'].append(f"Google Maps: {gmaps_result.get('error', 'Erreur inconnue')}")
        
        # Traiter les résultats Website
        website_result = results.get('website')
        if website_result and website_result is not None:
            if website_result.get('success', False):
                website_data = website_result.get('website_data')
                if website_data and isinstance(website_data, dict):
                    # Ajouter extraction_status pour compatibilité consolidator
                    website_data['extraction_status'] = 'success'
                    consolidated['website_data'] = website_data
                    consolidated['success'] = True
                    print(f"✅ Website data consolidée pour {hotel_data.get('name')}: {len(website_data)} champs")
                else:
                    print(f"⚠️ Website data vide pour {hotel_data.get('name')}")
            else:
                consolidated['errors'].append(f"Website: {website_result.get('error', 'Erreur inconnue')}")
                print(f"❌ Erreur website pour {hotel_data.get('name')}: {website_result.get('error')}")
        
        return consolidated
    
    def _create_error_result(self, hotel_data: Dict[str, str], error_message: str) -> Dict[str, Any]:
        """Crée un résultat d'erreur pour un hôtel
        
        Args:
            hotel_data (Dict[str, str]): Données de l'hôtel
            error_message (str): Message d'erreur
            
        Returns:
            Dict[str, Any]: Résultat d'erreur
        """
        
        return {
            'name': hotel_data['name'],
            'address': hotel_data.get('address', ''),
            'url': hotel_data.get('url', ''),
            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'success': False,
            'cvent_data': None,
            'gmaps_data': None,
            'errors': [error_message]
        }
    
    async def _safe_callback(self, callback: Callable, stats: Dict[str, Any]):
        """Exécute un callback de manière sécurisée
        
        Args:
            callback (Callable): Fonction de callback
            stats (Dict[str, Any]): Statistiques à passer au callback
        """
        
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(stats)
            else:
                callback(stats)
        except Exception as e:
            print(f"⚠️ Erreur callback: {e}")
    
    def get_current_stats(self) -> Optional[Dict[str, Any]]:
        """Retourne les statistiques actuelles si le traitement est en cours
        
        Returns:
            Optional[Dict[str, Any]]: Statistiques ou None
        """
        
        if self.progress_reporter and self._running:
            return self.progress_reporter.get_progress_stats()
        return None
    
    def is_running(self) -> bool:
        """Vérifie si le traitement est en cours
        
        Returns:
            bool: True si en cours
        """
        return self._running 