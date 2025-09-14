"""
Module d'extraction des données Cvent pour les salles de conférence
Intègre le code existant avec l'interface Streamlit
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
import tempfile

# Ajouter le dossier salles_cvent au path
sys.path.append(str(Path(__file__).parent.parent / "salles_cvent"))

from salles_cvent.detect_button import detect_button
# from salles_cvent.save_to_csv import save_to_csv  # Plus nécessaire avec Supabase
from salles_cvent.extract_data_popup import extract_data_popup
from salles_cvent.extract_data_grid import extract_data_grid


def extract_cvent_data(hotel_name, hotel_address, cvent_url, output_dir=None):
    """Extrait les données des salles de conférence depuis Cvent
    
    Args:
        hotel_name (str): Nom de l'hôtel
        hotel_address (str): Adresse de l'hôtel
        cvent_url (str): URL Cvent de l'hôtel
        output_dir (str): Dossier de sortie pour les fichiers CSV
        
    Returns:
        dict: Résultat de l'extraction avec métadonnées
        
    Raises:
        Exception: Erreurs d'extraction ou de navigation
    """
    
    print(f"🎯 Début extraction Cvent pour {hotel_name}")
    print(f"🔗 URL: {cvent_url}")
    
    # Le dossier de sortie n'est plus nécessaire avec Supabase
    # Path(output_dir).mkdir(exist_ok=True) if output_dir else None
    
    result = {
        'hotel_name': hotel_name,
        'hotel_address': hotel_address,
        'cvent_url': cvent_url,
        'extraction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'success': False,
        'error': None,
        'data': {
            'headers': [],
            'rows': [],
            'salles_count': 0,
            'interface_type': None,
            # 'csv_file': None  # Plus de fichier CSV avec Supabase
        }
    }
    
    try:
        with sync_playwright() as p:
            # Lancer le navigateur en mode headless pour Streamlit
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Aller sur la page Cvent
            print(f"📄 Chargement de la page: {cvent_url}")
            page.goto(cvent_url, timeout=30000)
            
            # Navigation vers l'onglet "Espace de réunion"
            # Plusieurs sélecteurs possibles pour l'onglet
            selectors = [
                'button:has-text("Espace de réunion")',
                'button:has-text("Meeting Space")',
                '[role="button"]:has-text("Espace de réunion")',
                'a:has-text("Espace de réunion")'
            ]
            
            clicked = False
            for selector in selectors:
                try:
                    if page.locator(selector).count() > 0:
                        page.locator(selector).first.click()
                        print("✅ Onglet 'Espace de réunion' ouvert")
                        clicked = True
                        break
                except:
                    continue
            
            if not clicked:
                print("ℹ️ Aucun onglet 'Espace de réunion' trouvé - hôtel sans salles de conférence")
                raise Exception("Impossible de trouver l'onglet 'Espace de réunion'")
            
            # Attendre que la page se charge
            page.wait_for_timeout(3000)
            
            # Détection du type d'interface et extraction
            interface_type = detect_button(page)
            result['data']['interface_type'] = interface_type
            
            if interface_type is None:
                raise Exception("Aucune interface de données détectée sur cette page Cvent")
            
            print(f"📋 Interface détectée: {interface_type}")
            
            if interface_type == "popup":
                print("📋 Interface POPUP détectée - extraction...")
                headers, data = extract_data_popup(page)

            elif interface_type == "popup_direct":
                print("📋 Interface POPUP directe détectée - extraction...")
                headers, data = extract_data_popup(page)

            elif interface_type == "grid":
                print("📋 Interface GRID détectée - extraction...")
                headers, data = extract_data_grid(page)

            elif interface_type == "grid_direct":
                print("📋 Interface GRID directe détectée - extraction...")
                headers, data = extract_data_grid(page)
                
            else:
                raise Exception(f"Type d'interface non reconnu: {interface_type}")
            
            # Validation des données extraites
            if not data or len(data) == 0:
                raise Exception("Aucune donnée extraite")
            
            if not headers or len(headers) == 0:
                raise Exception("Headers manquants")
            
            # Plus de sauvegarde CSV - les données sont retournées directement
            # pour être insérées dans Supabase

            # Remplir les résultats
            result['success'] = True
            result['data']['headers'] = headers
            result['data']['rows'] = data
            result['data']['salles_count'] = len(data)
            # result['data']['csv_file'] = None  # Plus de fichier CSV

            print(f"🎉 Extraction réussie: {len(data)} salles extraites")
            print(f"💾 Données prêtes pour insertion Supabase")
            
            browser.close()
            
    except Exception as e:
        result['error'] = str(e)
        print(f"❌ Erreur lors de l'extraction: {e}")
        
        # Fermer le navigateur en cas d'erreur
        try:
            browser.close()
        except:
            pass
    
    return result


def validate_cvent_url(url):
    """Valide une URL Cvent
    
    Args:
        url (str): URL à valider
        
    Returns:
        bool: True si l'URL est valide
    """
    
    if not url or not isinstance(url, str):
        return False
    
    # Vérifications de base
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Vérifier que c'est bien une URL Cvent
    if 'cvent.com' not in url.lower():
        return False
    
    return True


def get_extraction_summary(result):
    """Génère un résumé de l'extraction pour l'affichage
    
    Args:
        result (dict): Résultat de l'extraction
        
    Returns:
        dict: Résumé formaté pour l'affichage
    """
    
    summary = {
        'hotel_name': result['hotel_name'],
        'extraction_date': result['extraction_date'],
        'success': result['success'],
        'error_message': result.get('error'),
    }
    
    if result['success']:
        summary.update({
            'salles_count': result['data']['salles_count'],
            'interface_type': result['data']['interface_type'],
            # 'csv_file': result['data']['csv_file'],  # Plus de CSV
            'headers_count': len(result['data']['headers']),
            'sample_headers': result['data']['headers'][:5] if result['data']['headers'] else []
        })
    
    return summary


def create_consolidated_csv(extraction_results, output_file="outputs/consolidated_extractions.csv"):
    """Crée un CSV consolidé avec tous les résultats d'extraction
    
    Args:
        extraction_results (list): Liste des résultats d'extraction
        output_file (str): Chemin du fichier de sortie
        
    Returns:
        str: Chemin du fichier créé
    """
    
    # TODO: Implémenter la consolidation des données
    # Pour l'instant, création d'un fichier de métadonnées
    
    import pandas as pd
    
    consolidated_data = []
    for result in extraction_results:
        if result['success']:
            consolidated_data.append({
                'hotel_name': result['hotel_name'],
                'hotel_address': result['hotel_address'],
                'extraction_date': result['extraction_date'],
                'salles_count': result['data']['salles_count'],
                'interface_type': result['data']['interface_type'],
                'csv_file': result['data']['csv_file']
            })
    
    if consolidated_data:
        df = pd.DataFrame(consolidated_data)
        Path(output_file).parent.mkdir(exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8')
        return output_file
    
    return None 