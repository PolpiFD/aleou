"""
Composants UI réutilisables pour l'interface Streamlit
"""

import streamlit as st


def render_page_header():
    """Affiche le header principal de l'application"""
    st.title("🏨 Extracteur d'Informations Hôtelières")
    st.markdown("---")


def render_sidebar_stats():
    """Affiche les statistiques dans la sidebar"""
    st.header("📋 Fonctionnalités")
    st.markdown("""
    **Extractions disponibles :**
    - ✅ Salles de conférence (Cvent)
    - ✅ Informations Google Maps
    - ✅ Site web officiel (OpenAI GPT-4o-mini)
    
    **🏢 Aleou - Solution d'extraction hôtelière :**
    - Extraction multi-sources optimisée
    - Interface client simplifiée  
    - Traitement haute performance
    """)
    
    st.markdown("---")
    st.header("📊 Statistiques")
    
    # Initialiser les stats si nécessaire
    if 'extraction_stats' not in st.session_state:
        st.session_state.extraction_stats = {
            'total_hotels': 0,
            'successful_extractions': 0,
            'failed_extractions': 0
        }
    
    stats = st.session_state.extraction_stats
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total", stats['total_hotels'])
        st.metric("Succès", stats['successful_extractions'])
    
    with col2:
        st.metric("Échecs", stats['failed_extractions'])
        if stats['total_hotels'] > 0:
            success_rate = (stats['successful_extractions'] / stats['total_hotels']) * 100
            st.metric("Taux succès", f"{success_rate:.1f}%")


def render_mode_selector():
    """Affiche le sélecteur de mode d'extraction"""
    st.header("🎯 Mode d'extraction")
    
    return st.radio(
        "Choisissez votre mode d'extraction :",
        ["📁 Fichier CSV (multiple hôtels)", "🔗 URL unique"],
        horizontal=True
    )


def render_csv_format_instructions():
    """Affiche les instructions pour le format CSV"""
    with st.expander("📋 Format requis du fichier CSV"):
        st.markdown("""
        Votre fichier CSV doit contenir **exactement** ces colonnes :
        - `name` : Nom de l'hôtel
        - `adresse` : Adresse complète de l'hôtel
        - `URL` : URL Cvent de l'hôtel
        
        **Exemple :**
        ```
        name,adresse,URL
        Hôtel Example,123 Rue de la Paix Paris,https://cvent.com/venue/example
        ```
        """)


def render_csv_uploader():
    """Affiche l'interface d'upload CSV"""
    return st.file_uploader(
        "Choisir un fichier CSV",
        type=['csv'],
        help="Le fichier doit contenir les colonnes : name, adresse, URL"
    )


def render_extraction_options():
    """Affiche les options d'extraction disponibles"""
    st.subheader("⚙️ Options d'extraction")
    
    col1, col2 = st.columns(2)
    
    with col1:
        extract_cvent = st.checkbox("🏢 Salles de conférence (Cvent)", value=True)
        extract_gmaps = st.checkbox(
            "🗺️ Informations Google Maps", 
            value=False,
            help="Nécessite une clé API Google Maps configurée"
        )
    
    with col2:
        extract_website = st.checkbox(
            "🌐 Site web officiel", 
            value=False,
            help="Nécessite OPENAI_API_KEY configurée - Utilise uniquement Google Maps"
        )
    
    # Information sur la parallélisation
    if extract_gmaps or extract_website:
        st.info("🚀 Mode parallèle automatique pour > 3 hôtels ou avec extractions avancées")
    
    return {
        'cvent': extract_cvent,
        'gmaps': extract_gmaps,
        'website': extract_website
    }


def render_progress_bar(current: int, total: int, current_hotel: str):
    """Affiche une barre de progression avec le statut actuel"""
    progress = current / total if total > 0 else 0
    st.progress(progress)
    return st.empty().text(f"Traitement de {current_hotel} ({current}/{total})")


def render_consolidation_metrics(stats):
    """Affiche les métriques de consolidation"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total hôtels", stats['total_hotels'])
    
    with col2:
        st.metric(
            "Extractions réussies", 
            stats['successful_extractions'],
            delta=f"{stats.get('success_rate', 0):.1f}%"
        )
    
    with col3:
        st.metric("Extractions échouées", stats['failed_extractions'])
    
    with col4:
        st.metric("Total salles extraites", stats['total_rooms'])


def render_consolidation_status_message(stats):
    """Affiche le message de statut global de la consolidation"""
    if stats['successful_extractions'] > 0:
        if stats['failed_extractions'] == 0:
            st.success(f"🎉 Toutes les extractions ont réussi ! {stats['total_rooms']} salles extraites.")
        else:
            st.warning(f"⚠️ {stats['successful_extractions']} réussies, {stats['failed_extractions']} échouées")
    else:
        st.error("❌ Aucune extraction n'a réussi") 