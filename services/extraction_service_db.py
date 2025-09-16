"""
Service d'extraction avec Supabase - Nouvelle architecture
Remplace la consolidation CSV par l'insertion directe en DB
"""

import streamlit as st
import asyncio
import os
import time
from datetime import datetime
from typing import Dict, List, Any

import sys
from pathlib import Path

# Ajouter les modules au path
sys.path.append(str(Path(__file__).parent.parent))

from modules.database_service import DatabaseService
from modules.parallel_processor_db import ParallelHotelProcessorDB, ParallelConfig
from modules.supabase_client import SupabaseError


class ExtractionServiceDB:
    """Service principal pour les extractions avec Supabase"""

    def __init__(self):
        try:
            self.db_service = DatabaseService()
            self.session_id = None
        except SupabaseError as e:
            st.error(f"❌ Erreur configuration Supabase: {e}")
            st.info("🔧 Vérifiez SUPABASE_URL et SUPABASE_KEY dans votre .env")
            raise

    def process_csv_extraction(
        self,
        df,
        extract_gmaps: bool = False,
        extract_website: bool = False
    ):
        """Traite l'extraction pour un fichier CSV avec Supabase"""
        st.subheader("🔄 Extraction vers Supabase...")

        try:
            # Créer une session d'extraction
            csv_filename = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.session_id = self.db_service.create_new_session(
                csv_filename=csv_filename,
                total_hotels=len(df)
            )

            # Stocker dans l'état Streamlit pour persistance
            st.session_state['last_session_id'] = self.session_id

            st.info(f"📊 Session créée: {len(df)} hôtels à traiter")

            # Préparer les données
            hotels_data = []
            for _, row in df.iterrows():
                hotel_info = self._extract_hotel_info_from_row(row)
                hotels_data.append(hotel_info)

            # Utiliser le processeur parallèle DB
            config = ParallelConfig()
            processor = ParallelHotelProcessorDB(config)

            # Interface de suivi en temps réel
            progress_container = st.container()
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
                stats_placeholder = st.empty()

                # Tableau temps réel
                realtime_placeholder = st.empty()

                # Section téléchargement CSV
                download_section = st.empty()

            # Callback de progression
            def update_progress(stats):
                # Barre de progression
                progress = stats['progress_percent'] / 100
                progress_bar.progress(progress)

                # Status text
                eta_text = f" (ETA: {stats.get('eta_seconds', 0):.0f}s)" if stats.get('eta_seconds', 0) > 0 else ""
                status_text.text(
                    f"🔄 Batch {stats.get('batch_completed', 0)}/{stats.get('total_batches', 0)} "
                    f"| {stats['completed']}/{stats['total_hotels']} hôtels"
                    f" | Erreurs: {stats['errors']}{eta_text}"
                )

                # Stats détaillées
                with stats_placeholder:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Complétés", stats['completed'])
                    with col2:
                        st.metric("Erreurs", stats['errors'])
                    with col3:
                        st.metric("Progression", f"{stats['progress_percent']:.1f}%")
                    with col4:
                        st.metric("Temps écoulé", f"{stats.get('elapsed_time', 0):.0f}s")

                # Tableau temps réel depuis Supabase
                self._update_realtime_table(realtime_placeholder)

                # Bouton de téléchargement CSV dynamique
                self._update_download_section(download_section, stats)

            # Lancer le traitement
            try:
                final_stats = asyncio.run(
                    processor.process_hotels_to_database(
                        hotels_data=hotels_data,
                        session_id=self.session_id,
                        extract_cvent=True,
                        extract_gmaps=extract_gmaps,
                        extract_website=extract_website,
                        progress_callback=update_progress
                    )
                )

                # Finalisation (sans boutons d'export)
                self._display_final_results_simple(final_stats, progress_bar, status_text)

            except Exception as e:
                st.error(f"❌ Erreur traitement: {e}")
                self._cleanup_failed_session()

        except Exception as e:
            st.error(f"❌ Erreur création session: {e}")

    def process_single_url_extraction(
        self,
        name: str,
        address: str,
        url: str,
        extract_gmaps: bool = False,
        extract_website: bool = False
    ):
        """Traite l'extraction pour une URL unique avec Supabase"""
        st.subheader("🔄 Extraction URL unique vers Supabase...")

        try:
            # Créer une session pour l'URL unique
            session_name = f"URL_{name}_{datetime.now().strftime('%H%M%S')}"
            self.session_id = self.db_service.create_new_session(
                csv_filename=session_name,
                total_hotels=1
            )

            # Stocker dans l'état Streamlit pour persistance
            st.session_state['last_session_id'] = self.session_id

            hotel_data = {
                'name': name,
                'address': address,
                'url': url
            }

            # Traiter avec le processeur DB
            config = ParallelConfig()
            processor = ParallelHotelProcessorDB(config)

            # Interface simple pour URL unique
            with st.spinner(f"Extraction de {name}..."):
                final_stats = asyncio.run(
                    processor.process_hotels_to_database(
                        hotels_data=[hotel_data],
                        session_id=self.session_id,
                        extract_cvent=True,
                        extract_gmaps=extract_gmaps,
                        extract_website=extract_website
                    )
                )

            # Affichage des résultats
            if final_stats['successful'] > 0:
                st.success(f"✅ Extraction réussie pour {name}")
                st.info("💾 Données sauvegardées dans Supabase - Consultez l'onglet Exports pour télécharger")
            else:
                st.error(f"❌ Échec extraction pour {name}")

        except Exception as e:
            st.error(f"❌ Erreur extraction URL: {e}")

    def _extract_hotel_info_from_row(self, row) -> Dict[str, str]:
        """Extrait les informations d'hôtel depuis une ligne CSV"""
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

    def _update_realtime_table(self, placeholder):
        """Met à jour le tableau temps réel depuis Supabase"""
        if not self.session_id:
            return

        try:
            # Récupérer les stats depuis Supabase
            stats = self.db_service.get_session_statistics(self.session_id)

            with placeholder.container():
                st.subheader("📊 Progression en temps réel")

                if stats:
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric(
                            "Total",
                            stats.get('total_hotels', 0)
                        )
                    with col2:
                        st.metric(
                            "Complétés",
                            stats.get('completed', 0),
                            delta=f"+{stats.get('completed', 0)}"
                        )
                    with col3:
                        st.metric(
                            "En cours",
                            stats.get('processing', 0)
                        )
                    with col4:
                        st.metric(
                            "Échecs",
                            stats.get('failed', 0),
                            delta=f"+{stats.get('failed', 0)}" if stats.get('failed', 0) > 0 else None
                        )

                    # Graphique de progression
                    total = stats.get('total_hotels', 1)
                    completed = stats.get('completed', 0)
                    failed = stats.get('failed', 0)
                    pending = stats.get('pending', 0)
                    processing = stats.get('processing', 0)

                    progress_data = {
                        'Status': ['Complétés', 'Échecs', 'En cours', 'En attente'],
                        'Count': [completed, failed, processing, pending]
                    }

                    st.bar_chart(progress_data, x='Status', y='Count')

        except Exception as e:
            st.warning(f"Erreur mise à jour temps réel: {e}")

    def _update_download_section(self, placeholder, stats):
        """Met à jour la section de téléchargement CSV"""
        if not self.session_id:
            return

        try:
            # Récupérer les statistiques d'export
            export_stats = self.db_service.get_session_export_stats(self.session_id)

            with placeholder.container():
                st.subheader("📥 Téléchargement CSV")

                # Informations sur les données disponibles
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric(
                        "Hôtels traités",
                        export_stats.get('hotels_with_data', 0),
                        delta=f"sur {export_stats.get('total_hotels', 0)}"
                    )

                with col2:
                    st.metric(
                        "Salles extraites",
                        export_stats.get('total_rooms', 0)
                    )

                with col3:
                    # Statut d'export
                    if export_stats.get('export_ready', False):
                        st.success("✅ Données disponibles")
                    else:
                        st.info("⏳ En cours...")

                # Info sur les données disponibles
                if export_stats.get('total_rooms', 0) > 0:
                    st.info(f"💾 {export_stats['total_rooms']} salles extraites jusqu'à présent")
                    st.caption("📥 Consultez l'onglet **Exports** pour télécharger en continu")
                else:
                    st.info("ℹ️ Extraction en cours...")

                # Message d'aide
                st.caption("💡 Ce CSV contient toutes les données extraites jusqu'à présent. Vous pouvez l'interrompre et télécharger à tout moment.")

        except Exception as e:
            with placeholder.container():
                st.error(f"❌ Erreur section téléchargement: {e}")

    def _display_final_results(
        self,
        final_stats: Dict[str, Any],
        progress_bar,
        status_text
    ):
        """Affiche les résultats finaux"""
        progress_bar.progress(1.0)

        if final_stats['failed'] == 0:
            status_text.success("🎉 Extraction terminée avec succès!")
        else:
            status_text.warning(
                f"⚠️ Extraction terminée: {final_stats['failed']} échecs"
            )

        # Affichage des statistiques finales
        st.subheader("📊 Résultats finaux")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total traité",
                final_stats['total_hotels']
            )
        with col2:
            st.metric(
                "Succès",
                final_stats['successful'],
                delta=f"{(final_stats['successful']/final_stats['total_hotels']*100):.1f}%"
            )
        with col3:
            st.metric(
                "Échecs",
                final_stats['failed']
            )
        with col4:
            st.metric(
                "Temps total",
                f"{final_stats.get('elapsed_time', 0):.1f}s"
            )

        # Option d'export CSV depuis la DB
        self._display_export_options()

    def _display_hotel_data(self):
        """Affiche les données d'un hôtel depuis la DB"""
        if not self.session_id:
            return

        try:
            # Ici on pourrait récupérer et afficher les données
            # depuis Supabase pour vérification
            st.info("💾 Données sauvegardées dans Supabase")

            # TODO: Ajouter un aperçu des données insérées
            with st.expander("👁️ Aperçu des données"):
                st.info("Fonctionnalité d'aperçu en développement")

        except Exception as e:
            st.warning(f"Erreur affichage données: {e}")

    def _display_final_results_simple(
        self,
        final_stats: Dict[str, Any],
        progress_bar,
        status_text
    ):
        """Affiche les résultats finaux sans boutons d'export"""
        progress_bar.progress(1.0)

        if final_stats['failed'] == 0:
            status_text.success("🎉 Extraction terminée avec succès!")
        else:
            status_text.warning(
                f"⚠️ Extraction terminée: {final_stats['failed']} échecs"
            )

        # Affichage des statistiques finales
        st.subheader("📊 Résultats finaux")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total traité",
                final_stats['total_hotels']
            )
        with col2:
            st.metric(
                "Succès",
                final_stats['successful'],
                delta=f"{(final_stats['successful']/final_stats['total_hotels']*100):.1f}%"
            )
        with col3:
            st.metric(
                "Échecs",
                final_stats['failed']
            )
        with col4:
            st.metric(
                "Temps total",
                f"{final_stats.get('elapsed_time', 0):.1f}s"
            )

        # Message de redirection vers la page Exports
        st.success("✅ Extraction terminée!")
        st.info("📥 Consultez l'onglet **Exports** dans la navigation pour télécharger vos CSV")

    def _cleanup_failed_session(self):
        """Nettoie une session échouée"""
        if self.session_id:
            try:
                self.db_service.finalize_session(self.session_id, success=False)
            except:
                pass