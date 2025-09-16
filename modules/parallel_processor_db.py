"""
Module de traitement parallèle pour les extractions hôtelières avec Supabase
Version adaptée pour insertion directe en base de données
"""

import asyncio
import time
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime
import logging

from .cvent_extractor import extract_cvent_data
from .gmaps_extractor import extract_hotels_batch
from .website_extractor import extract_hotels_websites_batch
from .database_service import DatabaseService

# Configuration du logging
logger = logging.getLogger(__name__)


@dataclass
class ParallelConfig:
    """Configuration du traitement parallèle"""
    max_workers: int = 4
    batch_size: int = 10  # Batch de 10 comme demandé
    cvent_timeout: int = 45
    gmaps_timeout: int = 30
    website_timeout: int = 120

    @classmethod
    def from_machine_specs(cls, ram_gb: int = 16, cvent_only: bool = False):
        """Crée une configuration adaptée aux specs"""
        if cvent_only:
            if ram_gb >= 16:
                return cls(max_workers=2, batch_size=10, cvent_timeout=60)
            else:
                return cls(max_workers=1, batch_size=10, cvent_timeout=60)
        else:
            if ram_gb >= 16:
                return cls(max_workers=4, batch_size=10)
            else:
                return cls(max_workers=2, batch_size=10)


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

    def update_progress(self, success: bool = True):
        """Met à jour la progression globale"""
        with self.lock:
            self.completed += 1
            if not success:
                self.errors += 1

    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques"""
        with self.lock:
            elapsed = time.time() - self.start_time
            return {
                'total_hotels': self.total_hotels,
                'completed': self.completed,
                'errors': self.errors,
                'progress_percent': (self.completed / self.total_hotels * 100) if self.total_hotels > 0 else 0,
                'elapsed_time': elapsed,
                'eta_seconds': (elapsed / self.completed * (self.total_hotels - self.completed)) if self.completed > 0 else 0
            }


class ParallelHotelProcessorDB:
    """Processeur parallèle avec insertion directe en DB"""

    def __init__(self, config: ParallelConfig = None):
        self.config = config or ParallelConfig()
        self.progress_reporter = None
        self._running = False
        self.db_service = DatabaseService()
        self.session_id = None

    async def process_hotels_to_database(
        self,
        hotels_data: List[Dict[str, str]],
        session_id: str,
        extract_cvent: bool = True,
        extract_gmaps: bool = True,
        extract_website: bool = False,
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Traite les hôtels et insère directement en DB

        Args:
            hotels_data: Données des hôtels
            session_id: ID de la session Supabase
            extract_cvent: Activer extraction Cvent
            extract_gmaps: Activer extraction Google Maps
            extract_website: Activer extraction Website
            progress_callback: Callback de progression

        Returns:
            Dict: Statistiques finales
        """
        self.session_id = session_id
        self.progress_reporter = ProgressReporter(len(hotels_data))
        self._running = True

        logger.info(f"🚀 Démarrage: {len(hotels_data)} hôtels, session {session_id}")
        print(f"🚀 Traitement de {len(hotels_data)} hôtels par batch de {self.config.batch_size}")

        # Mettre à jour l'activité au démarrage pour éviter watchdog pendant extraction
        try:
            self.db_service.client.update_session_activity(session_id)
            logger.debug(f"Session {session_id}: activité mise à jour au démarrage")
        except Exception as e:
            logger.warning(f"Erreur MAJ activité démarrage: {e}")

        try:
            # Créer les batches
            batches = self._create_batches(hotels_data)
            total_success = 0
            total_errors = 0

            # Traiter chaque batch séquentiellement
            for batch_index, batch in enumerate(batches):
                logger.info(f"🚀 DÉBUT batch {batch_index + 1}/{len(batches)}: {len(batch)} hôtels")
                print(f"\n📦 Batch {batch_index + 1}/{len(batches)}: {len(batch)} hôtels")

                # Insérer les hôtels du batch dans la DB
                hotel_ids = self.db_service.prepare_hotels_batch(
                    session_id, batch
                )

                # Enrichir avec les IDs DB
                for i, hotel in enumerate(batch):
                    if i < len(hotel_ids):
                        hotel['hotel_id'] = hotel_ids[i]

                # Traiter le batch
                batch_results = await self._process_batch(
                    batch,
                    extract_cvent,
                    extract_gmaps,
                    extract_website,
                    progress_callback
                )

                # Sauvegarder en DB avec protection contre les crashes
                logger.info(f"💾 Sauvegarde batch {batch_index + 1} en DB...")
                try:
                    success, errors = self.db_service.process_batch_results(
                        batch_results
                    )
                    total_success += success
                    total_errors += errors
                    logger.info(f"✅ Batch {batch_index + 1} sauvegardé: {success} succès, {errors} échecs")
                except Exception as batch_db_error:
                    logger.error(f"❌ ERREUR critique sauvegarde batch {batch_index + 1}: {batch_db_error}")
                    print(f"❌ Erreur sauvegarde batch {batch_index + 1}: {batch_db_error}")
                    # Compter tous les hôtels du batch comme échoués
                    total_errors += len(batch)
                    # IMPORTANT: Ne pas crasher, continuer avec le batch suivant

                # Mettre à jour l'activité après chaque batch pour éviter watchdog
                try:
                    self.db_service.client.update_session_activity(session_id)
                    logger.debug(f"Session {session_id}: activité mise à jour après batch {batch_index + 1}")
                except Exception as e:
                    logger.warning(f"Erreur MAJ activité batch: {e}")

                # Callback de progression
                if progress_callback:
                    stats = self.progress_reporter.get_stats()
                    stats['batch_completed'] = batch_index + 1
                    stats['total_batches'] = len(batches)
                    await self._safe_callback(progress_callback, stats)

                # Log de fin de batch
                logger.info(f"✅ FIN batch {batch_index + 1}/{len(batches)} - Passage au suivant...")
                print(f"✅ Batch {batch_index + 1} terminé, passage au suivant...")

            # Finaliser la session
            logger.info(f"🏁 Finalisation session {session_id}: {total_success} succès, {total_errors} erreurs")
            print(f"🏁 Finalisation session: {total_success} succès, {total_errors} erreurs")

            try:
                self.db_service.finalize_session(
                    session_id,
                    success=(total_errors == 0)
                )
                print(f"✅ Session {session_id} finalisée avec succès")
            except Exception as e:
                logger.error(f"❌ Erreur finalisation session: {e}")
                print(f"❌ Erreur finalisation session: {e}")
                raise

            self._running = False

            # Stats finales
            final_stats = {
                'total_hotels': len(hotels_data),
                'successful': total_success,
                'failed': total_errors,
                'session_id': session_id,
                'elapsed_time': time.time() - self.progress_reporter.start_time
            }

            print(f"\n✅ Traitement terminé:")
            print(f"   • Succès: {total_success}/{len(hotels_data)}")
            print(f"   • Échecs: {total_errors}")
            print(f"   • Temps: {final_stats['elapsed_time']:.1f}s")

            return final_stats

        except Exception as e:
            self._running = False
            logger.error(f"💥 ERREUR CRITIQUE dans le processeur: {e}")
            print(f"💥 ERREUR CRITIQUE: {e}")

            # Essayer de finaliser la session même en cas d'erreur critique
            try:
                logger.warning(f"🔄 Tentative finalisation d'urgence session {session_id}")
                self.db_service.finalize_session(session_id, success=False)
                print(f"⚠️ Session {session_id} finalisée d'urgence (échouée)")
            except Exception as finalizer_error:
                logger.error(f"❌ Impossible de finaliser la session: {finalizer_error}")
                print(f"❌ Finalisation échouée: {finalizer_error}")

            raise

    def _create_batches(
        self,
        hotels_data: List[Dict[str, str]]
    ) -> List[List[Dict[str, str]]]:
        """Divise les hôtels en batches de taille fixe"""
        batches = []
        for i in range(0, len(hotels_data), self.config.batch_size):
            batch = hotels_data[i:i + self.config.batch_size]
            batches.append(batch)

        logger.info(f"📦 {len(batches)} batches créés")
        return batches

    async def _process_batch(
        self,
        batch: List[Dict[str, str]],
        extract_cvent: bool,
        extract_gmaps: bool,
        extract_website: bool,
        progress_callback: Optional[Callable]
    ) -> List[Dict[str, Any]]:
        """Traite un batch d'hôtels

        Args:
            batch: Batch d'hôtels
            extract_cvent: Activer Cvent
            extract_gmaps: Activer GMaps
            extract_website: Activer Website
            progress_callback: Callback

        Returns:
            List[Dict]: Résultats du batch
        """
        try:
            batch_results = []
            logger.info(f"🔄 Début traitement batch de {len(batch)} hôtels")

            # Traiter chaque hôtel du batch
            tasks = []
            for hotel_data in batch:
                task = self._process_single_hotel(
                    hotel_data,
                    extract_cvent,
                    extract_gmaps,
                    extract_website
                )
                tasks.append(task)

            # Exécuter en parallèle dans le batch
            logger.info(f"⚡ Lancement extraction parallèle {len(tasks)} hôtels")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"✅ Extraction parallèle terminée, consolidation des résultats...")

            # Traiter les résultats avec protection individuelle
            for i, result in enumerate(results):
                try:
                    hotel_data = batch[i]

                    if isinstance(result, Exception):
                        logger.error(f"Erreur hôtel {hotel_data.get('name')}: {result}")
                        batch_results.append({
                            'hotel_id': hotel_data.get('hotel_id'),
                            'name': hotel_data.get('name'),
                            'success': False,
                            'error': str(result)
                        })
                        # Protection du progress reporter
                        try:
                            self.progress_reporter.update_progress(success=False)
                        except Exception as progress_error:
                            logger.warning(f"Erreur progress reporter (failure): {progress_error}")
                    else:
                        batch_results.append(result)
                        # Protection du progress reporter
                        try:
                            self.progress_reporter.update_progress(success=True)
                        except Exception as progress_error:
                            logger.warning(f"Erreur progress reporter (success): {progress_error}")

                except Exception as consolidation_error:
                    logger.error(f"💥 Erreur consolidation résultat {i}: {consolidation_error}")
                    # Ajouter un résultat d'erreur par défaut
                    hotel_data = batch[i] if i < len(batch) else {'name': f'Hotel_{i}', 'hotel_id': None}
                    batch_results.append({
                        'hotel_id': hotel_data.get('hotel_id'),
                        'name': hotel_data.get('name', f'Hotel_{i}'),
                        'success': False,
                        'error': f'Erreur consolidation: {consolidation_error}'
                    })

            logger.info(f"🎯 Batch consolidé: {len(batch_results)} résultats")
            return batch_results

        except Exception as batch_error:
            logger.error(f"💥 ERREUR CRITIQUE dans _process_batch: {batch_error}")
            # Retourner des résultats d'erreur par défaut pour tous les hôtels
            fallback_results = []
            for hotel_data in batch:
                fallback_results.append({
                    'hotel_id': hotel_data.get('hotel_id'),
                    'name': hotel_data.get('name', 'Hotel_Unknown'),
                    'success': False,
                    'error': f'Crash batch: {batch_error}'
                })
            logger.warning(f"🚨 Retour de {len(fallback_results)} résultats fallback")
            return fallback_results

    async def _process_single_hotel(
        self,
        hotel_data: Dict[str, str],
        extract_cvent: bool,
        extract_gmaps: bool,
        extract_website: bool
    ) -> Dict[str, Any]:
        """Traite un seul hôtel

        Args:
            hotel_data: Données de l'hôtel
            extract_cvent: Activer Cvent
            extract_gmaps: Activer GMaps
            extract_website: Activer Website

        Returns:
            Dict: Résultat consolidé
        """
        result = {
            'hotel_id': hotel_data.get('hotel_id'),
            'name': hotel_data['name'],
            'success': False,
            'cvent_data': None,
            'gmaps_data': None,
            'website_data': None
        }

        try:
            # Phase 1: Extractions parallèles (Cvent + GMaps)
            tasks = {}

            if extract_cvent:
                tasks['cvent'] = self._extract_cvent_async(hotel_data)

            if extract_gmaps:
                tasks['gmaps'] = self._extract_gmaps_async(hotel_data)

            # Exécuter Phase 1
            if tasks:
                completed = await asyncio.gather(
                    *tasks.values(),
                    return_exceptions=True
                )

                for i, (task_name, _) in enumerate(tasks.items()):
                    task_result = completed[i]

                    if isinstance(task_result, Exception):
                        logger.warning(f"⚠️ {task_name} échoué pour {hotel_data['name']}: {task_result}")
                    else:
                        if task_name == 'cvent':
                            result['cvent_data'] = task_result
                        elif task_name == 'gmaps':
                            result['gmaps_data'] = task_result

            # Phase 2: Website (dépend de GMaps)
            if extract_website:
                website_url = None

                # Récupérer l'URL depuis GMaps
                if result.get('gmaps_data') and result['gmaps_data'].get('extraction_status') == 'success':
                    website_url = result['gmaps_data'].get('website')

                if website_url:
                    try:
                        website_result = await self._extract_website_async(
                            hotel_data, website_url
                        )
                        result['website_data'] = website_result
                    except Exception as e:
                        logger.warning(f"⚠️ Website échoué pour {hotel_data['name']}: {e}")

            result['success'] = True
            print(f"   ✅ {hotel_data['name']}: extraction complète")

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Erreur hôtel {hotel_data['name']}: {e}")

        return result

    async def _extract_cvent_async(
        self,
        hotel_data: Dict[str, str]
    ) -> Dict[str, Any]:
        """Extraction Cvent asynchrone"""
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
                result = await asyncio.wait_for(
                    future,
                    timeout=self.config.cvent_timeout
                )
                return result
            except asyncio.TimeoutError:
                raise Exception(f"Timeout Cvent ({self.config.cvent_timeout}s)")

    async def _extract_gmaps_async(
        self,
        hotel_data: Dict[str, str]
    ) -> Dict[str, Any]:
        """Extraction Google Maps asynchrone"""
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
            return result
        except asyncio.TimeoutError:
            raise Exception(f"Timeout GMaps ({self.config.gmaps_timeout}s)")

    async def _extract_website_async(
        self,
        hotel_data: Dict[str, str],
        website_url: str
    ) -> Dict[str, Any]:
        """Extraction Website asynchrone"""
        from .website_extractor import WebsiteExtractor

        try:
            extractor = WebsiteExtractor()
            result = await asyncio.wait_for(
                extractor.extract_hotel_website_data(
                    hotel_data['name'],
                    hotel_data.get('address', ''),
                    website_url
                ),
                timeout=self.config.website_timeout
            )
            return result
        except asyncio.TimeoutError:
            raise Exception(f"Timeout Website ({self.config.website_timeout}s)")

    async def _safe_callback(self, callback: Callable, stats: Dict[str, Any]):
        """Exécute un callback en toute sécurité"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(stats)
            else:
                callback(stats)
        except Exception as e:
            logger.warning(f"Erreur callback: {e}")

    def is_running(self) -> bool:
        """Vérifie si le traitement est en cours"""
        return self._running