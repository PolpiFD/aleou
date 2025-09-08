"""
Service d'extraction - Logique métier pure
Gère l'orchestration des extractions et la consolidation
"""

import streamlit as st
import asyncio
import os
from datetime import datetime
from typing import Dict, List, Any

import sys
from pathlib import Path

# Ajouter les modules au path
sys.path.append(str(Path(__file__).parent.parent))

from modules.cvent_extractor import extract_cvent_data, validate_cvent_url
from modules.data_consolidator import consolidate_hotel_extractions
from modules.parallel_processor import ParallelHotelProcessor, ParallelConfig


class ExtractionService:
    """Service principal pour les extractions d'hôtels"""
    
    def process_csv_extraction(self, df, extract_gmaps: bool = False, extract_website: bool = False):
        """Traite l'extraction pour un fichier CSV"""
        st.subheader("🔄 Extraction en cours...")
        
        # Déterminer si utiliser la parallélisation
        use_parallel = len(df) > 3 or extract_gmaps or extract_website
        
        if use_parallel:
            reasons = []
            if len(df) > 3:
                reasons.append(f"{len(df)} hôtels > 3")
            if extract_gmaps:
                reasons.append("Google Maps demandé")
            if extract_website:
                reasons.append("Website demandé")
            
            st.info(f"🚀 Mode parallèle activé: {' ou '.join(reasons)}")
            self._process_csv_parallel(df, extract_gmaps, extract_website)
        else:
            st.info(f"🔄 Mode séquentiel: {len(df)} hôtels ≤ 3 et pas d'extractions avancées")
            self._process_csv_sequential(df)
    
    def _process_csv_sequential(self, df):
        """Traite l'extraction CSV de manière séquentielle (petits volumes)"""
        progress_tracker = ProgressTracker(len(df))
        results_processor = ResultsProcessor()
        
        results = []
        
        for index, row in df.iterrows():
            hotel_info = self._extract_hotel_info_from_row(row)
            progress_tracker.update_progress(index + 1, hotel_info['name'])
            
            try:
                result = self._extract_single_hotel(hotel_info)
                results.append(result)
                
                progress_tracker.log_result_if_small_volume(hotel_info['name'], result)
                
            except Exception as e:
                error_result = self._create_error_result(hotel_info, str(e))
                results.append(error_result)
                progress_tracker.log_error_if_small_volume(hotel_info['name'], str(e))
        
        self._finalize_extraction(progress_tracker, results_processor, results)
    
    def _process_csv_parallel(self, df, extract_gmaps: bool, extract_website: bool):
        """Traite l'extraction CSV en parallèle (gros volumes ou avec extractions avancées)"""
        print(f"🔧 DEBUG - Paramètres reçus: extract_gmaps={extract_gmaps}, extract_website={extract_website}")
        st.info(f"🚀 Mode parallèle activé: {len(df)} hôtels avec 4 workers")
        
        # Configuration adaptée selon le type d'extraction
        is_cvent_only = not extract_gmaps and not extract_website
        config = ParallelConfig.from_machine_specs(16, cvent_only=is_cvent_only)  # MacBook Pro M1 16GB
        processor = ParallelHotelProcessor(config)
        
        if is_cvent_only:
            st.info(f"⚙️ Configuration Cvent optimisée: {config.max_workers} workers, batches de {config.batch_size}")
        else:
            extraction_types = []
            if extract_gmaps:
                extraction_types.append("Google Maps")
            if extract_website:
                extraction_types.append("Website")
            st.info(f"⚙️ Configuration mixte ({'+'.join(extraction_types)}): {config.max_workers} workers, batches de {config.batch_size}")
        
        # Préparer les données pour le traitement parallèle
        hotels_data = []
        for _, row in df.iterrows():
            hotel_info = self._extract_hotel_info_from_row(row)
            hotels_data.append(hotel_info)
        
        # Progress bar globale
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Callback de progression
        def update_progress(stats):
            progress = stats['progress_percent'] / 100
            progress_bar.progress(progress)
            
            eta_text = f" (ETA: {stats['eta_seconds']:.0f}s)" if stats['eta_seconds'] > 0 else ""
            status_text.text(
                f"🔄 {stats['completed']}/{stats['total_hotels']} hôtels traités"
                f" | Cvent: {stats['cvent_completed']} | GMaps: {stats['gmaps_completed']} | Website: {stats['website_completed']}"
                f" | Erreurs: {stats['errors']}{eta_text}"
            )
        
        # Lancement du traitement parallèle
        results = []  # Initialiser results pour le finally
        try:
            # Vérifier si Google Maps est configuré
            gmaps_available = extract_gmaps and self._check_gmaps_config()
            
            if extract_gmaps and not gmaps_available:
                st.warning("⚠️ Google Maps demandé mais API key non configurée. Extraction Cvent seulement.")
                extract_gmaps = False
            
            # Vérifier si Website est configuré
            website_available = extract_website and self._check_website_config()
            
            if extract_website and not website_available:
                st.error("⚠️ Website demandé mais API keys non configurées. Extraction sans Website.")
                st.info("🔧 Vérifiez que OPENAI_API_KEY est dans votre .env")
                extract_website = False
            elif extract_website and website_available:
                st.success("✅ Configuration website validée - Extraction activée")
                print(f"🔧 DEBUG - Website extraction activée: {extract_website}")
            
            print(f"🔧 DEBUG - Avant appel processeur: extract_website={extract_website}")
            results = asyncio.run(
                processor.process_hotels_parallel(
                    hotels_data,
                    extract_cvent=True,
                    extract_gmaps=extract_gmaps,
                    extract_website=extract_website,
                    progress_callback=update_progress
                )
            )
            
            # Finalisation
            progress_bar.progress(1.0)
            status_text.text("🔄 Consolidation des données...")
            
            # Debug pour la consolidation
            print(f"🔧 DEBUG - Résultats avant consolidation: {len(results)} hôtels")
            for i, result in enumerate(results[:2]):  # Afficher 2 premiers pour debug
                print(f"   Hôtel {i+1}: {result.get('name', 'N/A')} - Success: {result.get('success', 'N/A')}")
            
            # Consolidation avec données Google Maps et Website si disponibles
            consolidation_stats = consolidate_hotel_extractions(results, include_gmaps=extract_gmaps, include_website=extract_website)
            self._update_session_stats_parallel(consolidation_stats)
            
            # Vérifier si la consolidation a réussi
            if consolidation_stats['total_rooms'] > 0 or consolidation_stats['successful_extractions'] > 0:
                status_text.text("✅ Consolidation réussie !")
                ResultsProcessor.display_consolidation_results(consolidation_stats)
                # Remplacer complètement le texte de statut par le résultat final
                status_text.success("🎉 Extraction parallèle terminée avec succès !")
            else:
                status_text.error("⚠️ Consolidation échouée")
                st.warning("⚠️ Aucune donnée n'a pu être consolidée. Vérifiez les logs pour plus de détails.")
                st.info("Les fichiers individuels restent disponibles dans le dossier `outputs/`")
                
                # Afficher les statistiques de base quand même
                st.write("📊 **Statistiques d'extraction :**")
                st.write(f"- Hôtels traités : {len(results)}")
                st.write(f"- Succès Cvent : {sum(1 for r in results if r.get('cvent_data'))}")
                st.write(f"- Succès Google Maps : {sum(1 for r in results if r.get('gmaps_data'))}")
            
        except Exception as e:
            progress_bar.empty()
            status_text.error("❌ Erreur lors du traitement parallèle")
            st.error(f"❌ Erreur traitement parallèle: {e}")
            st.info("🔄 Repli vers traitement séquentiel...")
            
            # Repli vers le mode séquentiel 
            self._process_csv_sequential(df)
        
        finally:
            # Garantir qu'une consolidation est tentée même en cas d'erreur
            print("🔧 Consolidation d'urgence si nécessaire...")
            try:
                # Si results est vide ou non défini, tenter une récupération
                if not results:
                    print("⚠️ Aucun résultat disponible, tentative de récupération...")
                    results = self._recover_results_from_outputs()
                
                # Tenter la consolidation avec les résultats disponibles
                if results:
                    consolidation_stats = consolidate_hotel_extractions(
                        results, 
                        include_gmaps=extract_gmaps, 
                        include_website=extract_website
                    )
                    
                    # Afficher les résultats même partiels
                    if consolidation_stats.get('consolidation_file'):
                        status_text.success("✅ Fichier consolidé créé (mode récupération)")
                        ResultsProcessor.display_consolidation_results(consolidation_stats)
                    else:
                        self._create_emergency_consolidation_file()
                        st.warning("⚠️ Consolidation partielle - Vérifiez le dossier outputs/")
                else:
                    self._create_emergency_consolidation_file()
                    st.error("❌ Impossible de récupérer les résultats")
                    
            except Exception as consolidation_error:
                print(f"❌ Échec consolidation d'urgence: {consolidation_error}")
                st.error(f"❌ Erreur lors de la consolidation d'urgence: {consolidation_error}")
                st.info("Les fichiers individuels sont disponibles dans outputs/")
    
    def _check_gmaps_config(self) -> bool:
        """Vérifie si Google Maps est configuré
        
        Returns:
            bool: True si configuré
        """
        try:
            from modules.gmaps_extractor import GoogleMapsConfig
            GoogleMapsConfig.from_env()
            return True
        except ValueError:
            return False
    
    def _check_website_config(self) -> bool:
        """Vérifie si Website extraction (Firecrawl) est configuré
        
        Returns:
            bool: True si configuré
        """
        try:
            # 🔥 FIRECRAWL: Vérifier clé API Firecrawl (priorité)
            firecrawl_key = os.getenv('FIRECRAWL_API_KEY')
            if firecrawl_key:
                return True
            
            # Fallback: Vérifier OpenAI pour Legacy (optionnel)
            openai_key = os.getenv('OPENAI_API_KEY')
            if openai_key:
                print("⚠️ Firecrawl non configuré, utilisation Legacy LLM")
                return True
                
            return False
        except Exception:
            return False
    
    def process_single_url_extraction(self, name: str, address: str, url: str, 
                                     extract_gmaps: bool = False, extract_website: bool = False):
        """Traite l'extraction pour une URL unique"""
        st.subheader("🔄 Extraction en cours...")
        
        hotel_info = {'name': name, 'address': address, 'url': url}
        results_processor = ResultsProcessor()
        
        # Afficher les extractions activées
        extractions = ["Cvent"]
        if extract_gmaps:
            extractions.append("Google Maps")
        if extract_website:
            extractions.append("Site web")
        st.info(f"🔄 Extractions activées: {' + '.join(extractions)}")
        
        with st.spinner(f"Extraction des données pour {name}..."):
            try:
                if extract_gmaps or extract_website:
                    # Utiliser le processeur parallèle pour les extractions avancées
                    result = self._extract_single_hotel_complete(hotel_info, extract_gmaps, extract_website)
                else:
                    # Extraction Cvent simple
                    result = self._extract_single_hotel(hotel_info)
                results = [result]
            except Exception as e:
                error_result = self._create_error_result(hotel_info, str(e))
                results = [error_result]
        
        self._finalize_single_extraction(results_processor, results, extract_gmaps, extract_website)
    
    def _extract_hotel_info_from_row(self, row) -> Dict[str, str]:
        """Extrait les informations d'hôtel depuis une ligne CSV"""
        # Nettoyer les valeurs NaN qui deviennent des chaînes 'nan'
        def clean_value(value):
            if value is None:
                return ''
            str_value = str(value).strip()
            if str_value.lower() in ['nan', 'none', '']:
                return ''
            return str_value
        
        return {
            'name': clean_value(row['name']),
            'address': clean_value(row.get('adresse', '')),
            'url': clean_value(row.get('URL', ''))
        }
    
    def _extract_single_hotel(self, hotel_info: Dict[str, str]) -> Dict[str, Any]:
        """Extrait les données d'un seul hôtel"""
        if not validate_cvent_url(hotel_info['url']):
            raise Exception(f"URL Cvent invalide: {hotel_info['url']}")
        
        cvent_result = extract_cvent_data(
            hotel_info['name'], 
            hotel_info['address'], 
            hotel_info['url']
        )
        
        return self._format_extraction_result(hotel_info, cvent_result)
    
    def _extract_single_hotel_complete(self, hotel_info: Dict[str, str], 
                                      extract_gmaps: bool, extract_website: bool) -> Dict[str, Any]:
        """Extrait les données complètes d'un seul hôtel (Cvent + GMaps + Website)"""
        # Utiliser le processeur parallèle avec un seul hôtel
        config = ParallelConfig.from_machine_specs(16, cvent_only=False)
        processor = ParallelHotelProcessor(config)
        
        # Vérifier les configurations
        if extract_gmaps and not self._check_gmaps_config():
            st.warning("⚠️ Google Maps demandé mais API key non configurée")
            extract_gmaps = False
            
        if extract_website and not self._check_website_config():
            st.warning("⚠️ Website demandé but API keys non configurées")
            extract_website = False
        
        # Traitement avec le processeur parallèle
        results = asyncio.run(
            processor.process_hotels_parallel(
                [hotel_info],
                extract_cvent=True,
                extract_gmaps=extract_gmaps,
                extract_website=extract_website,
                progress_callback=None  # Pas de callback pour un seul hôtel
            )
        )
        
        return results[0] if results else self._create_error_result(hotel_info, "Échec extraction complète")
    
    def _format_extraction_result(self, hotel_info: Dict[str, str], cvent_result: Dict) -> Dict[str, Any]:
        """Formate le résultat d'extraction pour l'interface"""
        result = {
            'name': hotel_info['name'],
            'address': hotel_info['address'],
            'url': hotel_info['url'],
            'extraction_date': cvent_result['extraction_date'],
            'success': cvent_result['success'],
            'error': cvent_result.get('error')
        }
        
        if cvent_result['success']:
            result['cvent_data'] = {
                'salles_count': cvent_result['data']['salles_count'],
                'interface_type': cvent_result['data']['interface_type'],
                'data_file': cvent_result['data']['csv_file'],
                'headers': cvent_result['data']['headers'],
                'sample_data': cvent_result['data']['rows'][:3] if cvent_result['data']['rows'] else []
            }
        
        return result
    
    def _create_error_result(self, hotel_info: Dict[str, str], error_message: str) -> Dict[str, Any]:
        """Crée un résultat d'erreur standardisé"""
        return {
            'name': hotel_info['name'],
            'address': hotel_info['address'],
            'url': hotel_info['url'],
            'success': False,
            'error': error_message,
            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def _finalize_extraction(self, progress_tracker, results_processor, results):
        """Finalise l'extraction CSV avec consolidation"""
        progress_tracker.set_consolidation_status()
        
        consolidation_stats = consolidate_hotel_extractions(results)
        self._update_session_stats(consolidation_stats)
        
        results_processor.display_consolidation_results(consolidation_stats)
        progress_tracker.set_completion_status()
    
    def _finalize_single_extraction(self, results_processor, results, 
                                   extract_gmaps: bool = False, extract_website: bool = False):
        """Finalise l'extraction d'une URL unique"""
        consolidation_stats = consolidate_hotel_extractions(results, include_gmaps=extract_gmaps, include_website=extract_website)
        self._update_session_stats(consolidation_stats)
        results_processor.display_consolidation_results(consolidation_stats)
    
    def _update_session_stats(self, consolidation_stats):
        """Met à jour les statistiques de session"""
        st.session_state.extraction_stats['total_hotels'] += consolidation_stats['total_hotels']
        st.session_state.extraction_stats['successful_extractions'] += consolidation_stats['successful_extractions']
        st.session_state.extraction_stats['failed_extractions'] += consolidation_stats['failed_extractions']
    
    def _update_session_stats_parallel(self, consolidation_stats):
        """Met à jour les statistiques de session pour le traitement parallèle"""
        st.session_state.extraction_stats['total_hotels'] += consolidation_stats['total_hotels']
        st.session_state.extraction_stats['successful_extractions'] += consolidation_stats['successful_extractions']
        st.session_state.extraction_stats['failed_extractions'] += consolidation_stats['failed_extractions']
    
    def _recover_results_from_outputs(self):
        """Récupère les résultats depuis les fichiers outputs/ existants"""
        from glob import glob
        import os
        from datetime import datetime
        
        print("🔧 Tentative de récupération depuis outputs/...")
        results = []
        
        try:
            # Chercher tous les fichiers CSV récents
            today = datetime.now().strftime("%Y%m%d")
            files = glob(f'outputs/salles_*_{today}*.csv')
            
            if not files:
                # Chercher tous les fichiers CSV si aucun du jour
                files = glob('outputs/salles_*.csv')
            
            print(f"📄 {len(files)} fichiers trouvés dans outputs/")
            
            for f in files:
                try:
                    # Extraire le nom de l'hôtel depuis le nom du fichier
                    basename = os.path.basename(f)
                    # Format: salles_grid_NomHotel_20250908_123456.csv
                    parts = basename.replace('salles_grid_', '').replace('salles_popup_', '').split('_202')
                    hotel_name = parts[0] if parts else 'Unknown'
                    
                    results.append({
                        'name': hotel_name.replace('_', ' '),
                        'success': True,
                        'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'cvent_data': {
                            'data_file': f,
                            'salles_count': 1,  # Au moins une salle puisque le fichier existe
                            'interface_type': 'grid' if 'grid' in basename else 'popup',
                            'headers': [],
                            'sample_data': []
                        }
                    })
                except Exception as e:
                    print(f"⚠️ Erreur récupération {f}: {e}")
                    continue
            
            print(f"✅ {len(results)} résultats récupérés")
            return results
            
        except Exception as e:
            print(f"❌ Erreur récupération outputs: {e}")
            return []
    
    def _create_emergency_consolidation_file(self):
        """Crée un fichier de consolidation d'urgence minimal"""
        from datetime import datetime
        import pandas as pd
        import os
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            emergency_file = f'outputs/emergency_consolidation_{timestamp}.csv'
            
            # Créer un DataFrame minimal avec les informations disponibles
            data = {
                'status': ['emergency_consolidation'],
                'timestamp': [timestamp],
                'message': ['Consolidation automatique échouée - Vérifiez les fichiers individuels dans outputs/'],
                'hotels_processed': [st.session_state.extraction_stats.get('total_hotels', 0)],
                'successful': [st.session_state.extraction_stats.get('successful_extractions', 0)],
                'failed': [st.session_state.extraction_stats.get('failed_extractions', 0)]
            }
            
            df = pd.DataFrame(data)
            
            # Créer le dossier outputs s'il n'existe pas
            os.makedirs('outputs', exist_ok=True)
            
            # Sauvegarder le fichier
            df.to_csv(emergency_file, index=False)
            
            print(f"🚨 Fichier d'urgence créé: {emergency_file}")
            st.warning(f"📄 Fichier de consolidation d'urgence créé: {os.path.basename(emergency_file)}")
            
            # Proposer le téléchargement
            with open(emergency_file, 'r') as f:
                st.download_button(
                    label="📥 Télécharger le fichier d'urgence",
                    data=f.read(),
                    file_name=os.path.basename(emergency_file),
                    mime="text/csv"
                )
            
            return emergency_file
            
        except Exception as e:
            print(f"❌ Impossible de créer le fichier d'urgence: {e}")
            st.error(f"❌ Impossible de créer le fichier d'urgence: {e}")
            return None


class ProgressTracker:
    """Gère l'affichage de la progression"""
    
    def __init__(self, total_hotels: int):
        self.total_hotels = total_hotels
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
        self.show_logs = total_hotels <= 10
    
    def update_progress(self, current: int, current_hotel: str):
        """Met à jour la barre de progression"""
        progress = current / self.total_hotels
        self.progress_bar.progress(progress)
        self.status_text.text(f"Traitement de {current_hotel} ({current}/{self.total_hotels})")
    
    def log_result_if_small_volume(self, hotel_name: str, result: Dict):
        """Affiche les logs détaillés pour les petits volumes"""
        if self.show_logs:
            if result.get('success'):
                salles_count = result.get('cvent_data', {}).get('salles_count', 0)
                st.success(f"✅ {hotel_name}: {salles_count} salles extraites")
            else:
                st.error(f"❌ {hotel_name}: {result.get('error', 'Erreur inconnue')}")
    
    def log_error_if_small_volume(self, hotel_name: str, error_message: str):
        """Affiche les erreurs pour les petits volumes"""
        if self.show_logs:
            st.error(f"❌ {hotel_name}: {error_message}")
    
    def set_consolidation_status(self):
        """Affiche le statut de consolidation"""
        self.progress_bar.progress(1.0)
        self.status_text.text("🔄 Consolidation des données...")
    
    def set_completion_status(self):
        """Affiche le statut de completion"""
        self.status_text.text("✅ Extraction et consolidation terminées !")


class ResultsProcessor:
    """Gère l'affichage des résultats"""
    
    @staticmethod
    def display_consolidation_results(consolidation_stats):
        """Affiche les résultats de consolidation"""
        from ui.pages import ResultsDisplayPage
        ResultsDisplayPage.render_consolidation_results(consolidation_stats) 