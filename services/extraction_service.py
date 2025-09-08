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
            # 🔧 NOUVEAU: Batching automatique pour gros volumes
            if len(results) <= 200:
                # Comportement normal pour petits volumes (inchangé)
                print(f"📝 Mode consolidation normal ({len(results)} hôtels <= 200)")
                consolidation_stats = consolidate_hotel_extractions(results, include_gmaps=extract_gmaps, include_website=extract_website)
            else:
                # Mode batching pour gros volumes
                print(f"📦 Mode consolidation par batches ({len(results)} hôtels > 200)")
                consolidation_stats = self._consolidate_by_batches(results, extract_gmaps=extract_gmaps, extract_website=extract_website)
            
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
                # Pour les gros volumes (>500), essayer consolidation progressive
                if len(df) > 500:
                    print("📊 Gros volume détecté, tentative de consolidation progressive...")
                    consolidation_stats = self._progressive_consolidation(
                        extract_gmaps=extract_gmaps, 
                        extract_website=extract_website
                    )
                    if consolidation_stats and consolidation_stats.get('consolidation_file'):
                        status_text.success("✅ Consolidation progressive réussie")
                        ResultsProcessor.display_consolidation_results(consolidation_stats)
                        return
                
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
                    mime="text/csv",
                    key="emergency_download_btn"
                )
            
            return emergency_file
            
        except Exception as e:
            print(f"❌ Impossible de créer le fichier d'urgence: {e}")
            st.error(f"❌ Impossible de créer le fichier d'urgence: {e}")
            return None
    
    def _progressive_consolidation(self, extract_gmaps: bool = True, extract_website: bool = True):
        """Consolidation progressive par batches pour éviter les fuites mémoire"""
        from modules.data_consolidator import consolidate_hotel_extractions
        from glob import glob
        import os
        import pandas as pd
        from datetime import datetime
        
        print("🔄 Démarrage de la consolidation progressive...")
        
        try:
            # Récupérer tous les fichiers de sortie par batch
            output_files = glob('outputs/salles_*.csv')
            print(f"📄 {len(output_files)} fichiers trouvés dans outputs/")
            
            if not output_files:
                print("❌ Aucun fichier de sortie trouvé")
                return None
            
            # Traitement par petits batches pour éviter la surcharge mémoire
            BATCH_SIZE = 100
            all_consolidated_data = []
            batch_num = 0
            
            # Grouper les fichiers par batches
            for i in range(0, len(output_files), BATCH_SIZE):
                batch_files = output_files[i:i + BATCH_SIZE]
                batch_num += 1
                print(f"🔄 Traitement du batch {batch_num}: {len(batch_files)} fichiers")
                
                # Simuler des résultats pour ce batch
                batch_results = []
                for file_path in batch_files:
                    try:
                        basename = os.path.basename(file_path)
                        # Extraire le nom de l'hôtel depuis le nom du fichier
                        parts = basename.replace('salles_grid_', '').replace('salles_popup_', '').split('_202')
                        hotel_name = parts[0].replace('_', ' ') if parts else 'Unknown'
                        
                        batch_results.append({
                            'name': hotel_name,
                            'success': True,
                            'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'cvent_data': {
                                'data_file': file_path,
                                'salles_count': 1,
                                'interface_type': 'grid' if 'grid' in basename else 'popup',
                                'headers': [],
                                'sample_data': []
                            }
                        })
                    except Exception as e:
                        print(f"⚠️ Erreur lecture fichier {file_path}: {e}")
                        continue
                
                if batch_results:
                    # Consolider ce batch
                    batch_stats = consolidate_hotel_extractions(
                        batch_results, 
                        include_gmaps=extract_gmaps, 
                        include_website=extract_website
                    )
                    
                    # Si consolidation réussie pour ce batch
                    if batch_stats.get('consolidation_file') and os.path.exists(batch_stats['consolidation_file']):
                        print(f"✅ Batch {batch_num} consolidé: {batch_stats['consolidation_file']}")
                        
                        # Lire les données consolidées de ce batch
                        try:
                            batch_df = pd.read_csv(batch_stats['consolidation_file'])
                            all_consolidated_data.append(batch_df)
                        except Exception as e:
                            print(f"⚠️ Erreur lecture batch consolidé {batch_num}: {e}")
            
            # Fusionner tous les batches consolidés
            if all_consolidated_data:
                print(f"🔗 Fusion de {len(all_consolidated_data)} batches consolidés...")
                final_df = pd.concat(all_consolidated_data, ignore_index=True)
                
                # Créer le fichier final
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                final_file = f'outputs/consolidation_progressive_{timestamp}.csv'
                final_df.to_csv(final_file, index=False)
                
                print(f"✅ Consolidation progressive terminée: {final_file}")
                
                # Retourner les stats finales
                return {
                    'consolidation_file': final_file,
                    'total_hotels': len(final_df),
                    'total_salles': len(final_df),
                    'successful_extractions': len(final_df),
                    'failed_extractions': 0,
                    'processing_mode': 'progressive'
                }
            else:
                print("❌ Aucun batch consolidé avec succès")
                return None
                
        except Exception as e:
            print(f"❌ Erreur lors de la consolidation progressive: {e}")
            return None
    
    def _consolidate_by_batches(self, results, extract_gmaps: bool = True, extract_website: bool = True):
        """Consolide par batches de 200 hôtels avec fichiers progressifs disponibles
        Crée un fichier téléchargeable après chaque batch pour éviter les pertes
        
        Args:
            results: Liste complète des résultats d'extraction
            extract_gmaps: Inclure données Google Maps
            extract_website: Inclure données Website
        
        Returns:
            Dict: Statistiques de consolidation finale
        """
        import streamlit as st
        from modules.data_consolidator import consolidate_hotel_extractions
        
        try:
            BATCH_SIZE = 200
            total_hotels = len(results)
            all_batch_files = []
            cumulative_results = []  # Résultats cumulés pour fichiers progressifs
            
            combined_stats = {
                'total_hotels': total_hotels,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'total_rooms': 0,
                'hotels_with_data': [],
                'failed_hotels': [],
                'consolidation_file': None,
                'preview_data': [],
                'unique_headers': set(),
                'consolidation_date': ''
            }
            
            print(f"🔄 Consolidation progressive par batches de {BATCH_SIZE} hôtels...")
            
            # Traiter chaque batch
            for i in range(0, total_hotels, BATCH_SIZE):
                batch = results[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                total_batches = (total_hotels - 1) // BATCH_SIZE + 1
                
                print(f"   📦 Batch {batch_num}/{total_batches}: {len(batch)} hôtels")
                
                # Filtrer les résultats avec au moins une extraction réussie
                valid_batch = []
                for result in batch:
                    has_cvent = result.get('cvent_data', {}).get('salles_count', 0) > 0
                    has_gmaps = result.get('gmaps_data', {}).get('extraction_status') == 'success'
                    has_website = result.get('website_data') and len(result.get('website_data', {})) > 0
                    
                    if has_cvent or has_gmaps or has_website:
                        valid_batch.append(result)
                
                if not valid_batch:
                    print(f"   ⚠️ Batch {batch_num} ignoré - aucune donnée exploitable")
                    continue
                
                # Ajouter au cumul pour consolidation progressive
                cumulative_results.extend(valid_batch)
                
                print(f"   📊 Consolidation cumulative: {len(cumulative_results)} hôtels avec données")
                
                # Consolider CUMULATIVEMENT (tous les résultats depuis le début)
                progressive_stats = consolidate_hotel_extractions(
                    cumulative_results, 
                    include_gmaps=extract_gmaps, 
                    include_website=extract_website
                )
                
                if progressive_stats and progressive_stats.get('consolidation_file'):
                    # Renommer le fichier pour indiquer qu'il est progressif
                    import os
                    from datetime import datetime
                    
                    old_file = progressive_stats['consolidation_file']
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    hotels_count = len(cumulative_results)
                    
                    # Nouveau nom: hotels_progressive_1-400_timestamp.csv
                    base_dir = os.path.dirname(old_file)
                    progressive_filename = f"hotels_progressive_1-{hotels_count}_{timestamp}.csv"
                    progressive_path = os.path.join(base_dir, progressive_filename)
                    
                    # Renommer le fichier
                    os.rename(old_file, progressive_path)
                    progressive_stats['consolidation_file'] = progressive_path
                    
                    # 🚀 MISE À JOUR EN TEMPS RÉEL DE L'INTERFACE
                    if 'progressive_consolidation_file' not in st.session_state:
                        st.session_state.progressive_consolidation_file = None
                    
                    st.session_state.progressive_consolidation_file = progressive_path
                    st.session_state.progressive_hotels_count = hotels_count
                    st.session_state.progressive_stats = progressive_stats
                    
                    print(f"   ✅ Fichier progressif créé: {progressive_filename}")
                    print(f"      📈 {progressive_stats['successful_extractions']} extractions, {progressive_stats['total_rooms']} salles")
                    
                    # Accumuler les statistiques
                    combined_stats = progressive_stats.copy()
                    combined_stats['total_hotels'] = total_hotels  # Garder le total original
                    
                    all_batch_files.append(progressive_path)
                
                else:
                    print(f"   ❌ Échec consolidation batch {batch_num}")
            
            # Le dernier fichier est le fichier final
            if all_batch_files:
                combined_stats['consolidation_file'] = all_batch_files[-1]  # Dernier fichier = plus complet
                print(f"✅ Consolidation progressive terminée: {len(all_batch_files)} fichiers créés")
                print(f"📄 Fichier final: {os.path.basename(combined_stats['consolidation_file'])}")
            else:
                print("❌ Aucun fichier consolidé créé")
                
            return combined_stats
            
        except Exception as e:
            print(f"❌ Erreur consolidation progressive: {e}")
            import traceback
            traceback.print_exc()
            return combined_stats
    
    def _merge_csv_files(self, csv_files):
        """Fusionne plusieurs fichiers CSV en un seul
        
        Args:
            csv_files: Liste des chemins vers les fichiers CSV à fusionner
        
        Returns:
            str: Chemin vers le fichier fusionné
        """
        from datetime import datetime
        import pandas as pd
        import os
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            merged_file = f'outputs/hotels_consolidation_merged_{timestamp}.csv'
            
            print(f"🔗 Fusion en cours vers {merged_file}...")
            
            first_file = True
            total_rows = 0
            
            for i, csv_file in enumerate(csv_files):
                if os.path.exists(csv_file):
                    try:
                        df = pd.read_csv(csv_file, encoding='utf-8')
                        total_rows += len(df)
                        
                        # Premier fichier: créer avec headers
                        if first_file:
                            df.to_csv(merged_file, index=False, encoding='utf-8', mode='w')
                            first_file = False
                            print(f"   📄 Fichier 1/{len(csv_files)}: {len(df)} lignes (headers)")
                        else:
                            # Fichiers suivants: append sans headers
                            df.to_csv(merged_file, index=False, encoding='utf-8', mode='a', header=False)
                            print(f"   📄 Fichier {i+1}/{len(csv_files)}: {len(df)} lignes (append)")
                        
                        # Nettoyer le fichier temporaire après fusion
                        os.remove(csv_file)
                        print(f"   🗑️ Fichier temporaire supprimé: {os.path.basename(csv_file)}")
                        
                    except Exception as e:
                        print(f"⚠️ Erreur traitement {csv_file}: {e}")
                        continue
            
            print(f"✅ Fusion terminée: {total_rows} lignes totales")
            return merged_file
            
        except Exception as e:
            print(f"❌ Erreur fusion CSV: {e}")
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