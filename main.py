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


def render_main_layout():
    """Affiche la layout principale de l'application"""
    render_page_header()
    
    with st.sidebar:
        render_sidebar_stats()
    
    mode = render_mode_selector()
    return mode


def route_to_appropriate_page(mode: str):
    """Route vers la page appropriée selon le mode sélectionné"""
    if mode == "📁 Fichier CSV (multiple hôtels)":
        csv_page = CSVExtractionPage()
        csv_page.render()
    else:
        url_page = SingleURLExtractionPage()
        url_page.render()


def main():
    """Point d'entrée principal de l'application"""
    configure_streamlit_page()
    mode = render_main_layout()
    route_to_appropriate_page(mode)


if __name__ == "__main__":
    main() 