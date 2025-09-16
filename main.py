"""
Interface principale de l'application d'extraction d'informations hôtelières
Point d'entrée streamlit simplifié selon les principes Clean Code
"""

import streamlit as st
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Configuration des imports
sys.path.append(str(Path(__file__).parent / "salles_cvent"))

from ui.components import render_page_header, render_sidebar_stats, render_mode_selector
from ui.pages import CSVExtractionPage, SingleURLExtractionPage


def configure_streamlit_page():
    """Configure la page Streamlit avec les paramètres de base"""
    st.set_page_config(
        page_title="🏨 Extracteur d'Informations Hôtelières",
        page_icon="🏨",
        layout="wide",
        initial_sidebar_state="expanded"
    )


def render_main_navigation():
    """Affiche la navigation principale de l'application"""
    with st.sidebar:
        st.title("🏨 Navigation")

        # Navigation principale
        main_page = st.radio(
            "Choisir une section :",
            ["🔄 Extraction", "📥 Exports"],
            horizontal=False
        )

        st.markdown("---")
        render_sidebar_stats()

    return main_page


def render_extraction_layout():
    """Affiche la layout de la page extraction"""
    render_page_header()
    mode = render_mode_selector()
    return mode


def route_to_extraction_page(mode: str):
    """Route vers la page d'extraction appropriée"""
    if mode == "📁 Fichier CSV (multiple hôtels)":
        csv_page = CSVExtractionPage()
        csv_page.render()
    else:
        url_page = SingleURLExtractionPage()
        url_page.render()


def render_exports_page():
    """Affiche la page d'exports"""
    from ui.pages import ExportsPage
    render_page_header()
    exports_page = ExportsPage()
    exports_page.render()


def main():
    """Point d'entrée principal de l'application"""
    configure_streamlit_page()

    # Navigation principale
    main_page = render_main_navigation()

    if main_page == "🔄 Extraction":
        # Page d'extraction
        mode = render_extraction_layout()
        route_to_extraction_page(mode)
    elif main_page == "📥 Exports":
        # Page d'exports
        render_exports_page()


if __name__ == "__main__":
    main() 