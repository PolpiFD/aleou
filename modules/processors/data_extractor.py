"""
Extracteur de données - Gère les différentes sources d'extraction en parallèle
"""

import asyncio
from typing import List, Dict, Any, Tuple
import time

from .hotel_processor import HotelProcessor
from ...config.settings import settings


class DataExtractor:
    """Gestionnaire d'extraction de données avec parallélisation optimisée"""
    
    def __init__(self):
        self.stats = {
            'total_hotels': 0,
            'successful_hotels': 0,
            'failed_hotels': 0,
            'total_time': 0,
            'avg_time_per_hotel': 0,
            'extraction_stats': {
                'cvent': {'success': 0, 'failed': 0, 'avg_time': 0},
                'gmaps': {'success': 0, 'failed': 0, 'avg_time': 0},
                'website': {'success': 0, 'failed': 0, 'avg_time': 0}
            }
        }
    
    async def extract_hotels_parallel(self, hotels_data: List[Dict[str, Any]],
                                    enable_cvent: bool = True,
                                    enable_gmaps: bool = True,
                                    enable_website: bool = True,
                                    progress_callback: callable = None) -> List[Dict[str, Any]]:
        """
        Extrait les données de plusieurs hôtels en parallèle
        
        Args:
            hotels_data: Liste des données d'hôtels à traiter
            enable_cvent: Activer extraction Cvent
            enable_gmaps: Activer extraction Google Maps  
            enable_website: Activer extraction website
            progress_callback: Fonction appelée à chaque hôtel traité
            
        Returns:
            Liste des résultats d'extraction
        """
        start_time = time.time()
        self.stats['total_hotels'] = len(hotels_data)
        
        print(f"🚀 Début extraction parallèle: {len(hotels_data)} hôtels")
        print(f"⚙️ Configuration: Cvent={enable_cvent}, GMaps={enable_gmaps}, Website={enable_website}")
        print(f"👥 Workers: {settings.parallel.max_workers}, Batch size: {settings.parallel.batch_size}")
        
        # Diviser en batches pour contrôler la charge
        batches = self._create_batches(hotels_data, settings.parallel.batch_size)
        all_results = []
        
        for batch_num, batch in enumerate(batches, 1):
            print(f"\n📦 Batch {batch_num}/{len(batches)}: {len(batch)} hôtels")
            
            batch_results = await self._process_batch(
                batch, enable_cvent, enable_gmaps, enable_website, progress_callback
            )
            all_results.extend(batch_results)
            
            # Pause courte entre batches pour éviter la surcharge
            if batch_num < len(batches):
                await asyncio.sleep(1)
        
        # Calculer statistiques finales
        self._calculate_final_stats(all_results, time.time() - start_time)
        
        return all_results
    
    async def _process_batch(self, batch: List[Dict[str, Any]],
                           enable_cvent: bool, enable_gmaps: bool, enable_website: bool,
                           progress_callback: callable = None) -> List[Dict[str, Any]]:
        """Traite un batch d'hôtels en parallèle"""
        
        # Créer les tâches pour ce batch
        tasks = []
        for hotel_data in batch:
            processor = HotelProcessor()
            task = asyncio.create_task(
                processor.process_hotel(hotel_data, enable_cvent, enable_gmaps, enable_website)
            )
            tasks.append(task)
        
        # Exécuter avec limite de concurrence
        semaphore = asyncio.Semaphore(settings.parallel.max_workers)
        
        async def process_with_semaphore(task):
            async with semaphore:
                return await task
        
        # Attendre tous les résultats du batch
        batch_results = await asyncio.gather(
            *[process_with_semaphore(task) for task in tasks],
            return_exceptions=True
        )
        
        # Traiter les résultats et exceptions
        processed_results = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                # Gestion d'exception
                error_result = {
                    'hotel_data': batch[i],
                    'success': False,
                    'errors': [f"Exception: {str(result)}"],
                    'processing_time': 0
                }
                processed_results.append(error_result)
                print(f"❌ Exception pour {batch[i].get('name', 'Unknown')}: {result}")
            else:
                processed_results.append(result)
                
                # Callback de progression
                if progress_callback:
                    progress_callback(result)
        
        return processed_results
    
    def _create_batches(self, items: List[Any], batch_size: int) -> List[List[Any]]:
        """Divise une liste en batches de taille donnée"""
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i + batch_size])
        return batches
    
    def _calculate_final_stats(self, results: List[Dict[str, Any]], total_time: float):
        """Calcule les statistiques finales d'extraction"""
        self.stats['total_time'] = total_time
        self.stats['avg_time_per_hotel'] = total_time / len(results) if results else 0
        
        # Compter succès/échecs
        successful = 0
        extraction_times = {'cvent': [], 'gmaps': [], 'website': []}
        
        for result in results:
            if result.get('success'):
                successful += 1
            
            # Collecter les temps d'extraction par type
            if result.get('cvent_data'):
                extraction_times['cvent'].append(result.get('processing_time', 0))
                if result['cvent_data'].get('success'):
                    self.stats['extraction_stats']['cvent']['success'] += 1
                else:
                    self.stats['extraction_stats']['cvent']['failed'] += 1
            
            if result.get('gmaps_data'):
                extraction_times['gmaps'].append(result.get('processing_time', 0))
                if result['gmaps_data'].get('success'):
                    self.stats['extraction_stats']['gmaps']['success'] += 1
                else:
                    self.stats['extraction_stats']['gmaps']['failed'] += 1
            
            if result.get('website_data'):
                extraction_times['website'].append(result.get('processing_time', 0))
                if result['website_data'].get('success'):
                    self.stats['extraction_stats']['website']['success'] += 1
                else:
                    self.stats['extraction_stats']['website']['failed'] += 1
        
        self.stats['successful_hotels'] = successful
        self.stats['failed_hotels'] = len(results) - successful
        
        # Calculer temps moyens par type
        for extraction_type, times in extraction_times.items():
            if times:
                self.stats['extraction_stats'][extraction_type]['avg_time'] = sum(times) / len(times)
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des performances"""
        total = self.stats['total_hotels']
        successful = self.stats['successful_hotels']
        success_rate = (successful / total * 100) if total > 0 else 0
        
        return {
            'total_hotels': total,
            'successful_hotels': successful,
            'success_rate': f"{success_rate:.1f}%",
            'total_time': f"{self.stats['total_time']:.1f}s",
            'avg_time_per_hotel': f"{self.stats['avg_time_per_hotel']:.1f}s",
            'throughput': f"{total / self.stats['total_time']:.1f} hôtels/s" if self.stats['total_time'] > 0 else "0 hôtels/s",
            'extraction_breakdown': {
                name: {
                    'success_rate': f"{(stats['success'] / (stats['success'] + stats['failed']) * 100):.1f}%" 
                    if (stats['success'] + stats['failed']) > 0 else "0%",
                    'avg_time': f"{stats['avg_time']:.1f}s"
                }
                for name, stats in self.stats['extraction_stats'].items()
            }
        }
    
    def print_performance_summary(self):
        """Affiche un résumé des performances"""
        summary = self.get_performance_summary()
        
        print(f"\n📊 RÉSUMÉ DES PERFORMANCES:")
        print(f"   🏨 Hôtels traités: {summary['total_hotels']}")
        print(f"   ✅ Succès: {summary['successful_hotels']} ({summary['success_rate']})")
        print(f"   ⏱️ Temps total: {summary['total_time']}")
        print(f"   📈 Throughput: {summary['throughput']}")
        print(f"   ⚡ Temps moyen/hôtel: {summary['avg_time_per_hotel']}")
        
        print(f"\n🔍 DÉTAIL PAR TYPE D'EXTRACTION:")
        for extraction_type, stats in summary['extraction_breakdown'].items():
            print(f"   {extraction_type.upper()}: {stats['success_rate']} succès, {stats['avg_time']} moyen")