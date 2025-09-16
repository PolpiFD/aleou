"""
Pages de l'application Streamlit
Gère les workflows d'extraction CSV et URL unique
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime

from ui.components import (
    render_csv_format_instructions, 
    render_csv_uploader, 
    render_extraction_options
)
from services.extraction_service_db import ExtractionServiceDB


class CSVExtractionPage:
    """Page d'extraction pour fichiers CSV"""

    def __init__(self):
        try:
            self.extraction_service = ExtractionServiceDB()
        except Exception as e:
            st.error(f"❌ Erreur initialisation service: {e}")
            st.info("🔧 Vérifiez votre configuration Supabase dans .env")
            self.extraction_service = None
    
    def render(self):
        """Affiche la page d'extraction CSV"""
        st.subheader("📁 Upload de fichier CSV")
        
        render_csv_format_instructions()
        uploaded_file = render_csv_uploader()
        
        if uploaded_file is not None:
            self._handle_uploaded_file(uploaded_file)
    
    def _handle_uploaded_file(self, uploaded_file):
        """Traite le fichier CSV uploadé"""
        try:
            # Essayer différents encodages
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    uploaded_file.seek(0)  # Reset file pointer
                    df = pd.read_csv(uploaded_file, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Impossible de décoder le fichier CSV. Veuillez le sauvegarder en UTF-8.")
            
            if self._validate_csv_format(df):
                self._show_csv_preview(df)
                self._handle_extraction_options(df)
        
        except Exception as e:
            st.error(f"❌ Erreur lors de la lecture du fichier : {str(e)}")
    
    def _validate_csv_format(self, df):
        """Valide le format du CSV"""
        required_columns = ['name', 'adresse', 'URL']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"❌ Colonnes manquantes dans le CSV : {', '.join(missing_columns)}")
            st.info("💡 Colonnes requises : name, adresse, URL")
            return False
        
        st.success(f"✅ Fichier valide : {len(df)} hôtel(s) détecté(s)")
        return True
    
    def _show_csv_preview(self, df):
        """Affiche l'aperçu du CSV"""
        with st.expander("👁️ Aperçu des données"):
            st.dataframe(df.head())
    
    def _handle_extraction_options(self, df):
        """Gère les options d'extraction et lance le processus"""
        options = render_extraction_options()
        
        if st.button("🚀 Lancer l'extraction", type="primary", use_container_width=True):
            if not self.extraction_service:
                st.error("❌ Service d'extraction non disponible")
                return

            if options['cvent'] or options['gmaps'] or options.get('website', False):
                with st.spinner("Préparation de l'extraction..."):
                    self.extraction_service.process_csv_extraction(
                        df,
                        extract_gmaps=options['gmaps'],
                        extract_website=options.get('website', False)
                    )
            else:
                st.warning("⚠️ Aucune extraction sélectionnée")


class SingleURLExtractionPage:
    """Page d'extraction pour URL unique"""

    def __init__(self):
        try:
            self.extraction_service = ExtractionServiceDB()
        except Exception as e:
            st.error(f"❌ Erreur initialisation service: {e}")
            st.info("🔧 Vérifiez votre configuration Supabase dans .env")
            self.extraction_service = None
    
    def render(self):
        """Affiche la page d'extraction URL unique"""
        st.subheader("🔗 Extraction d'une URL unique")
        
        # Formulaire pour la saisie des données
        with st.form("single_url_form"):
            hotel_data = self._get_hotel_form_data()
            options = self._get_extraction_options()
            
            submitted = st.form_submit_button(
                "🚀 Lancer l'extraction", 
                type="primary", 
                use_container_width=True
            )
        
        # Traitement des résultats EN DEHORS du formulaire
        if submitted:
            if not self.extraction_service:
                st.error("❌ Service d'extraction non disponible")
                return
            self._handle_form_submission(hotel_data, options)
    
    def _get_hotel_form_data(self):
        """Récupère les données du formulaire hôtel"""
        col1, col2 = st.columns(2)
        
        with col1:
            hotel_name = st.text_input("🏨 Nom de l'hôtel", placeholder="Ex: Hôtel de la Paix")
            hotel_address = st.text_input("📍 Adresse", placeholder="Ex: 123 Rue de la Paix, Paris")
        
        with col2:
            cvent_url = st.text_input("🔗 URL Cvent", placeholder="https://cvent.com/venue/...")
        
        return {
            'name': hotel_name,
            'address': hotel_address,
            'url': cvent_url
        }
    
    def _get_extraction_options(self):
        """Récupère les options d'extraction du formulaire"""
        st.markdown("### ⚙️ Options d'extraction")
        col3, col4 = st.columns(2)
        
        with col3:
            extract_cvent = st.checkbox("🏢 Salles de conférence (Cvent)", value=True)
            extract_gmaps = st.checkbox(
                "🗺️ Informations Google Maps", 
                value=False,
                help="Nécessite une clé API Google Maps configurée"
            )
        
        with col4:
            extract_website = st.checkbox(
                "🌐 Site web officiel", 
                value=False,
                help="Nécessite OPENAI_API_KEY configurée - Utilise uniquement Google Maps"
            )
        
        return {
            'cvent': extract_cvent,
            'gmaps': extract_gmaps,
            'website': extract_website
        }
    
    def _handle_form_submission(self, hotel_data, options):
        """Traite la soumission du formulaire"""
        if not self._validate_form_data(hotel_data):
            return
        
        if options['cvent'] or options['gmaps'] or options['website']:
            with st.spinner(f"Extraction de {hotel_data['name']}..."):
                self.extraction_service.process_single_url_extraction(
                    hotel_data['name'],
                    hotel_data['address'],
                    hotel_data['url'],
                    extract_gmaps=options['gmaps'],
                    extract_website=options['website']
                )
        else:
            st.warning("⚠️ Aucune extraction sélectionnée")
    
    def _validate_form_data(self, hotel_data):
        """Valide les données du formulaire"""
        if not hotel_data['name'].strip():
            st.error("❌ Le nom de l'hôtel est requis")
            return False
        
        if not hotel_data['url'].strip():
            st.error("❌ L'URL Cvent est requise")
            return False
        
        if not hotel_data['url'].startswith('http'):
            st.error("❌ L'URL doit commencer par http:// ou https://")
            return False
        
        return True


class ResultsDisplayPage:
    """Page d'affichage des résultats"""
    
    @staticmethod
    def render_consolidation_results(consolidation_stats):
        """Affiche les résultats de consolidation"""
        from ui.components import render_consolidation_metrics, render_consolidation_status_message
        
        st.subheader("📊 Résultats de consolidation")
        
        render_consolidation_metrics(consolidation_stats)
        render_consolidation_status_message(consolidation_stats)
        
        if consolidation_stats['successful_extractions'] == 0:
            ResultsDisplayPage._show_error_details(consolidation_stats)
            return
        
        ResultsDisplayPage._show_consolidation_file(consolidation_stats)
        ResultsDisplayPage._show_preview_data(consolidation_stats)
        ResultsDisplayPage._show_hotel_details(consolidation_stats)
        ResultsDisplayPage._show_error_summary(consolidation_stats)
    
    @staticmethod
    def _show_error_details(consolidation_stats):
        """Affiche les détails des erreurs si aucune extraction n'a réussi"""
        if consolidation_stats.get('failed_hotels'):
            st.subheader("🔍 Erreurs détectées")
            for i, failed in enumerate(consolidation_stats['failed_hotels'][:5]):
                st.error(f"**{failed['name']}**: {failed['error']}")
            
            if len(consolidation_stats['failed_hotels']) > 5:
                st.info(f"... et {len(consolidation_stats['failed_hotels']) - 5} autres erreurs")
    
    @staticmethod
    def _show_consolidation_file(consolidation_stats):
        """Affiche les informations du fichier consolidé"""
        if not consolidation_stats.get('consolidation_file'):
            st.warning("⚠️ Aucun fichier consolidé généré")
            return
        
        st.subheader("📄 Fichier consolidé généré")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.info(f"**Fichier :** {os.path.basename(consolidation_stats['consolidation_file'])}")
            st.info(f"**Colonnes :** {consolidation_stats.get('unique_columns', 0)} colonnes détectées")
            st.info(f"**Date :** {consolidation_stats.get('consolidation_date', '')}")
        
        with col2:
            ResultsDisplayPage._render_download_button(consolidation_stats)
    
    @staticmethod
    def _render_download_button(consolidation_stats):
        """Affiche le bouton de téléchargement"""
        try:
            with open(consolidation_stats['consolidation_file'], 'r', encoding='utf-8') as f:
                csv_content = f.read()
            
            st.download_button(
                label="📥 Télécharger le fichier consolidé",
                data=csv_content,
                file_name=os.path.basename(consolidation_stats['consolidation_file']),
                mime="text/csv",
                type="primary",
                use_container_width=True
            )
        except Exception as e:
            st.error(f"Erreur lecture fichier: {e}")
    
    @staticmethod
    def _show_preview_data(consolidation_stats):
        """Affiche l'aperçu des données"""
        if not consolidation_stats.get('preview_data'):
            return
        
        with st.expander("👁️ Aperçu du fichier consolidé", expanded=True):
            try:
                df_preview = pd.DataFrame(consolidation_stats['preview_data'])
                st.dataframe(df_preview, use_container_width=True)
                
                if len(consolidation_stats['preview_data']) == 10:
                    st.info("💡 Aperçu limité aux 10 premières lignes")
            except Exception as e:
                st.error(f"Erreur affichage aperçu: {e}")
    
    @staticmethod
    def _show_hotel_details(consolidation_stats):
        """Affiche les détails par hôtel pour les petits volumes"""
        if consolidation_stats['total_hotels'] > 20 or not consolidation_stats.get('hotels_with_data'):
            return
        
        with st.expander("📋 Détails par hôtel", expanded=False):
            for hotel in consolidation_stats['hotels_with_data']:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**{hotel['name']}**")
                with col2:
                    st.write(f"{hotel['rooms_count']} salles")
                with col3:
                    st.write(f"Interface: {hotel['interface_type']}")
    
    @staticmethod
    def _show_error_summary(consolidation_stats):
        """Affiche le résumé des erreurs"""
        if not consolidation_stats.get('failed_hotels'):
            return
        
        with st.expander(f"❌ Erreurs ({len(consolidation_stats['failed_hotels'])})", expanded=False):
            for failed in consolidation_stats['failed_hotels']:
                st.error(f"**{failed['name']}**: {failed['error']}")


class ExportsPage:
    """Page dédiée aux exports CSV depuis Supabase"""

    def __init__(self):
        try:
            from modules.database_service import DatabaseService
            self.db_service = DatabaseService()
        except Exception as e:
            st.error(f"❌ Erreur initialisation service: {e}")
            st.info("🔧 Vérifiez votre configuration Supabase dans .env")
            self.db_service = None

    def render(self):
        """Affiche la page d'exports"""
        st.subheader("📥 Historique des Extractions")

        if not self.db_service:
            st.error("❌ Service de base de données non disponible")
            return

        # Récupérer les 10 dernières sessions
        sessions = self._get_recent_sessions()

        if not sessions:
            st.info("ℹ️ Aucune extraction trouvée")
            st.caption("💡 Lancez une extraction depuis l'onglet **Extraction** pour voir vos sessions ici")
            return

        # Afficher chaque session
        for session in sessions:
            self._render_session_card(session)

    def _get_recent_sessions(self):
        """Récupère les 10 dernières sessions d'extraction"""
        try:
            result = self.db_service.client.client.table("extraction_sessions")\
                .select("*")\
                .order("created_at", desc=True)\
                .limit(10)\
                .execute()

            return result.data
        except Exception as e:
            st.error(f"❌ Erreur récupération sessions: {e}")
            return []

    def _render_session_card(self, session):
        """Affiche une carte de session avec boutons d'export"""
        with st.container():
            # Header de la session
            col1, col2, col3 = st.columns([3, 2, 2])

            with col1:
                session_date = session.get('created_at', '')[:19].replace('T', ' ')
                st.markdown(f"### 🗓️ {session_date}")
                st.caption(f"**Fichier:** {session.get('csv_filename', 'N/A')}")

            with col2:
                total_hotels = session.get('total_hotels', 0)
                processed = session.get('processed_hotels', 0)
                st.metric("Hôtels", f"{processed}/{total_hotels}")

            with col3:
                status = session.get('status', 'unknown')
                status_emoji = {
                    'completed': '✅',
                    'processing': '⏳',
                    'failed': '❌'
                }.get(status, '❓')
                st.metric("Statut", f"{status_emoji} {status.title()}")

            # Boutons d'export
            if status in ['completed', 'processing']:
                self._render_export_buttons(session['id'])
            else:
                st.warning("⚠️ Session échouée - Exports non disponibles")

            st.markdown("---")

    def _render_export_buttons(self, session_id):
        """Affiche les boutons d'export pour une session"""
        col1, col2 = st.columns(2)

        try:
            # Générer les CSV
            csv_complete = self._generate_csv_from_view(session_id, include_empty_rooms=True)
            csv_rooms_only = self._generate_csv_from_view(session_id, include_empty_rooms=False)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            with col1:
                if csv_complete:
                    st.download_button(
                        label="📊 Export Complet",
                        data=csv_complete,
                        file_name=f"export_complet_{session_id[:8]}_{timestamp}.csv",
                        mime="text/csv",
                        key=f"complete_{session_id}",
                        use_container_width=True,
                        help="Toutes les données: Cvent + Google Maps + Website"
                    )
                else:
                    st.button("📊 Export Complet", disabled=True, use_container_width=True)
                    st.caption("Aucune donnée")

            with col2:
                if csv_rooms_only:
                    st.download_button(
                        label="🏢 Export Salles",
                        data=csv_rooms_only,
                        file_name=f"salles_seulement_{session_id[:8]}_{timestamp}.csv",
                        mime="text/csv",
                        key=f"rooms_{session_id}",
                        use_container_width=True,
                        type="secondary",
                        help="Uniquement les hôtels avec salles de réunion"
                    )
                else:
                    st.button("🏢 Export Salles", disabled=True, use_container_width=True)
                    st.caption("Aucune salle")

        except Exception as e:
            st.error(f"❌ Erreur génération CSV: {e}")

    def _generate_csv_from_view(self, session_id, include_empty_rooms=True):
        """Génère le CSV depuis la vue consolidée"""
        try:
            # Utiliser la vue consolidée si elle existe
            try:
                result = self.db_service.client.client.table("consolidated_export")\
                    .select("*")\
                    .eq("session_id", session_id)\
                    .execute()

                if result.data:
                    df = pd.DataFrame(result.data)

                    # Filtrer selon les options
                    if not include_empty_rooms:
                        df = df[df['nom_salle'].notna() & (df['nom_salle'] != '')]

                    return df.to_csv(index=False, encoding='utf-8')

            except Exception:
                # Fallback sur l'ancienne méthode si la vue n'existe pas
                return self.db_service.export_session_to_csv(
                    session_id=session_id,
                    include_empty_rooms=include_empty_rooms
                )

        except Exception as e:
            st.error(f"Erreur export CSV: {e}")
            return None 