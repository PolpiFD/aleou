"""
Module de consolidation des données d'extraction hôtelière
Fusionne toutes les extractions en un seul fichier CSV consolidé
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import csv
from typing import List, Dict, Any


def consolidate_hotel_extractions(extraction_results: List[Dict], output_dir: str = "outputs", include_gmaps: bool = False, include_website: bool = False) -> Dict[str, Any]:
    """Consolide toutes les extractions d'hôtels en un seul CSV
    
    Args:
        extraction_results (List[Dict]): Liste des résultats d'extraction
        output_dir (str): Dossier de sortie
        include_gmaps (bool): Inclure les données Google Maps
        include_website (bool): Inclure les données des sites web
        
    Returns:
        Dict[str, Any]: Informations sur la consolidation
    """
    
    print("🔄 Début de la consolidation des données...")
    
    # Créer le dossier de sortie
    Path(output_dir).mkdir(exist_ok=True)
    
    # Statistiques
    stats = {
        'total_hotels': len(extraction_results),
        'successful_extractions': 0,
        'failed_extractions': 0,
        'total_rooms': 0,
        'hotels_with_data': [],
        'failed_hotels': [],
        'consolidation_file': None,
        'preview_data': [],
        'unique_headers': set(),
        'consolidation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Données consolidées
    consolidated_data = []
    
    for result in extraction_results:
        hotel_name = result.get('name', 'Hotel_Unknown')
        hotel_address = result.get('address', '')
        
        # Vérifier si au moins une extraction a réussi (Cvent, Google Maps ou Website)
        cvent_result = result.get('cvent_data')
        has_cvent_data = (cvent_result is not None and 
                         cvent_result.get('salles_count', 0) > 0)
        
        has_gmaps_data = result.get('gmaps_data') is not None and result.get('gmaps_data', {}).get('extraction_status') == 'success'
        
        has_website_data = (result.get('website_data') is not None and 
                           isinstance(result.get('website_data'), dict) and
                           len(result.get('website_data', {})) > 0)
        
        if has_cvent_data or has_gmaps_data or has_website_data:
            stats['successful_extractions'] += 1
            
            # Traitement des données Cvent (salles de réunion)
            if has_cvent_data:
                cvent_data = result['cvent_data']
                headers = cvent_data.get('headers', [])
                rows = cvent_data.get('sample_data', [])  # On va charger toutes les données, pas juste sample
                
                # Charger les données complètes depuis le fichier CSV
                data_file = cvent_data.get('data_file')
                if data_file and os.path.exists(data_file):
                    try:
                        df_hotel = pd.read_csv(data_file, encoding='utf-8')
                        headers = df_hotel.columns.tolist()
                        rows = df_hotel.values.tolist()
                        print(f"✅ {hotel_name}: {len(rows)} salles chargées depuis {os.path.basename(data_file)}")
                    except Exception as e:
                        print(f"⚠️ Erreur lecture {data_file}: {e}")
                        # Utiliser les données sample en fallback
                
                # Ajouter les métadonnées d'hôtel à chaque ligne de salle
                for row in rows:
                    if isinstance(row, list) and len(row) > 0:
                        consolidated_row = create_base_hotel_row(hotel_name, hotel_address, result)
                        
                        # Ajouter toutes les données de la salle Cvent
                        for i, header in enumerate(headers):
                            if i < len(row):
                                clean_header = clean_header_name(header)
                                consolidated_row[clean_header] = row[i]
                                stats['unique_headers'].add(clean_header)
                        
                        # Ajouter les données Google Maps si disponibles
                        if has_gmaps_data and include_gmaps:
                            gmaps_data = result['gmaps_data']
                            add_gmaps_data_to_row(consolidated_row, gmaps_data, stats)
                        
                        # Ajouter les données Website si disponibles
                        if has_website_data and include_website:
                            website_data = result['website_data']
                            add_website_data_to_row(consolidated_row, website_data, stats)
                        
                        consolidated_data.append(consolidated_row)
                        stats['total_rooms'] += 1
                
                stats['hotels_with_data'].append({
                    'name': hotel_name,
                    'rooms_count': len(rows),
                    'interface_type': cvent_data.get('interface_type', ''),
                    'file': os.path.basename(cvent_data.get('data_file', '')) if cvent_data.get('data_file') else 'no_csv'
                })
            
            # Si seulement Google Maps et/ou Website est disponible (pas de salles)
            elif (has_gmaps_data and include_gmaps) or (has_website_data and include_website):
                consolidated_row = create_base_hotel_row(hotel_name, hotel_address, result)
                
                # Ajouter Google Maps data si disponible
                if has_gmaps_data and include_gmaps:
                    gmaps_data = result['gmaps_data']
                    add_gmaps_data_to_row(consolidated_row, gmaps_data, stats)
                
                # Ajouter Website data si disponible
                if has_website_data and include_website:
                    website_data = result['website_data']
                    add_website_data_to_row(consolidated_row, website_data, stats)
                
                consolidated_data.append(consolidated_row)
                
                # Déterminer le type d'interface
                interface_types = []
                if has_gmaps_data and include_gmaps:
                    interface_types.append('gmaps')
                if has_website_data and include_website:
                    interface_types.append('website')
                
                stats['hotels_with_data'].append({
                    'name': hotel_name,
                    'rooms_count': 0,
                    'interface_type': '_'.join(interface_types) + '_only',
                    'file': '_'.join(interface_types) + '_data'
                })
            
        else:
            stats['failed_extractions'] += 1
            stats['failed_hotels'].append({
                'name': hotel_name,
                'address': hotel_address,
                'error': result.get('error', 'Erreur inconnue')
            })
    
    # Créer le CSV consolidé
    if consolidated_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        consolidation_filename = f"hotels_consolidation_{timestamp}.csv"
        consolidation_path = os.path.join(output_dir, consolidation_filename)
        
        # Créer le DataFrame
        df_consolidated = pd.DataFrame(consolidated_data)
        
        # 🔧 CORRECTION - Réorganiser colonnes + gérer doublons
        df_consolidated = organize_columns_and_clean_duplicates(df_consolidated)
        
        # 🔧 NOUVEAU - Nettoyer les types pour éviter l'erreur PyArrow
        df_consolidated = clean_data_types_for_display(df_consolidated)
        
        # Sauvegarder
        df_consolidated.to_csv(consolidation_path, index=False, encoding='utf-8')
        
        stats['consolidation_file'] = consolidation_path
        stats['preview_data'] = df_consolidated.head(10).to_dict('records')
        
        print(f"✅ Consolidation terminée: {len(consolidated_data)} salles de {stats['successful_extractions']} hôtels")
        print(f"📄 Fichier consolidé: {consolidation_filename}")
        
        # Créer aussi un fichier de statistiques
        create_stats_file(stats, output_dir, timestamp)
        
    else:
        print("❌ Aucune donnée à consolider")
    
    return stats


def clean_header_name(header: str) -> str:
    """Nettoie et standardise les noms de headers
    
    Args:
        header (str): Nom de header original
        
    Returns:
        str: Nom de header nettoyé
    """
    
    if not header or not isinstance(header, str):
        return "unknown_column"
    
    # Nettoyer et standardiser
    cleaned = header.strip()
    
    # 🔧 CORRECTION: Mapping EXHAUSTIF des headers GRID/POPUP
    header_mapping = {
        # NOMS DE SALLE - Tout vers 'salle_nom'
        'Salles de réunion': 'salle_nom',
        'Nom': 'salle_nom',
        'nom': 'salle_nom',
        
        # TAILLE - Tout vers 'salle_taille' 
        'Taille de la salle': 'salle_taille',
        'Taille': 'salle_taille',
        'taille': 'salle_taille',
        
        # HAUTEUR - Uniformiser
        'Hauteur du plafond': 'hauteur_plafond',
        'hauteur du plafond': 'hauteur_plafond',
        
        # DIMENSIONS
        'Dimensions de la salle': 'dimensions',
        'dimensions': 'dimensions',
        
        # 🎯 CAPACITÉ MAXIMUM - Tout vers 'capacite_maximum'
        'Capacité maximum': 'capacite_maximum',
        'Capacité max': 'capacite_maximum', 
        'Capacité maximale': 'capacite_maximum',
        'capacité maximum': 'capacite_maximum',
        'capacité max': 'capacite_maximum',
        'capacité maximale': 'capacite_maximum',
        
        # AUTRES CAPACITÉS - Uniformiser casse
        'En U': 'capacite_en_u',
        'en u': 'capacite_en_u',
        'En banquet': 'capacite_banquet',
        'en banquet': 'capacite_banquet',
        'En cocktail': 'capacite_cocktail', 
        'en cocktail': 'capacite_cocktail',
        'Théâtre': 'capacite_theatre',
        'théâtre': 'capacite_theatre',
        'Salle de classe': 'capacite_classe',
        'salle de classe': 'capacite_classe',
        'Salle de conférence': 'capacite_conference',
        'salle de conférence': 'capacite_conference',
        'Demi-lune (Cabaret)': 'capacite_cabaret',
        'Demi-lune': 'capacite_cabaret',
        'demi-lune': 'capacite_cabaret'
    }
    
    # Utiliser le mapping si disponible
    if cleaned in header_mapping:
        return header_mapping[cleaned]
    
    # 🎯 FALLBACK: Si pas dans le mapping, normaliser intelligemment
    cleaned_lower = cleaned.lower()
    
    # Détection intelligente par mots-clés
    if any(word in cleaned_lower for word in ['nom', 'salle']) and 'réunion' in cleaned_lower:
        return 'salle_nom'
    elif 'taille' in cleaned_lower:
        return 'salle_taille' 
    elif 'hauteur' in cleaned_lower and 'plafond' in cleaned_lower:
        return 'hauteur_plafond'
    elif 'capacité' in cleaned_lower and any(word in cleaned_lower for word in ['max', 'maximum', 'maximale']):
        return 'capacite_maximum'
    elif cleaned_lower == 'en u' or 'u' in cleaned_lower:
        return 'capacite_en_u'
    elif 'banquet' in cleaned_lower:
        return 'capacite_banquet'
    elif 'cocktail' in cleaned_lower:
        return 'capacite_cocktail'
    elif 'théâtre' in cleaned_lower or 'theatre' in cleaned_lower:
        return 'capacite_theatre'
    elif 'classe' in cleaned_lower:
        return 'capacite_classe'
    elif 'conférence' in cleaned_lower or 'conference' in cleaned_lower:
        return 'capacite_conference'
    elif 'demi' in cleaned_lower and 'lune' in cleaned_lower:
        return 'capacite_cabaret'
    
    # Sinon, nettoyer le nom normalement
    cleaned = cleaned_lower
    cleaned = cleaned.replace(' ', '_')
    cleaned = cleaned.replace('(', '').replace(')', '')
    cleaned = cleaned.replace('-', '_')
    cleaned = cleaned.replace('/', '_')
    
    return cleaned


def create_stats_file(stats: Dict, output_dir: str, timestamp: str):
    """Crée un fichier de statistiques détaillées
    
    Args:
        stats (Dict): Statistiques de consolidation
        output_dir (str): Dossier de sortie
        timestamp (str): Timestamp pour le nom de fichier
    """
    
    stats_filename = f"consolidation_stats_{timestamp}.txt"
    stats_path = os.path.join(output_dir, stats_filename)
    
    with open(stats_path, 'w', encoding='utf-8') as f:
        f.write("📊 STATISTIQUES DE CONSOLIDATION\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"📅 Date: {stats['consolidation_date']}\n\n")
        
        f.write("🎯 RÉSUMÉ GLOBAL:\n")
        f.write(f"  • Total hôtels traités: {stats['total_hotels']}\n")
        f.write(f"  • Extractions réussies: {stats['successful_extractions']}\n")
        f.write(f"  • Extractions échouées: {stats['failed_extractions']}\n")
        f.write(f"  • Total salles extraites: {stats['total_rooms']}\n")
        f.write(f"  • Taux de succès: {(stats['successful_extractions']/stats['total_hotels']*100):.1f}%\n\n")
        
        if stats['hotels_with_data']:
            f.write("✅ HÔTELS AVEC DONNÉES:\n")
            for hotel in stats['hotels_with_data']:
                f.write(f"  • {hotel['name']}: {hotel['rooms_count']} salles ({hotel['interface_type']})\n")
            f.write("\n")
        
        if stats['failed_hotels']:
            f.write("❌ HÔTELS ÉCHOUÉS:\n")
            for hotel in stats['failed_hotels']:
                f.write(f"  • {hotel['name']}: {hotel['error']}\n")
            f.write("\n")
        
        f.write("📋 COLONNES DÉTECTÉES:\n")
        for header in sorted(stats['unique_headers']):
            f.write(f"  • {header}\n")
    
    print(f"📄 Statistiques sauvegardées: {stats_filename}")


def get_consolidation_summary(stats: Dict) -> Dict[str, Any]:
    """Génère un résumé de consolidation pour l'interface
    
    Args:
        stats (Dict): Statistiques de consolidation
        
    Returns:
        Dict[str, Any]: Résumé formaté pour l'affichage
    """
    
    summary = {
        'total_hotels': stats['total_hotels'],
        'successful_extractions': stats['successful_extractions'],
        'failed_extractions': stats['failed_extractions'],
        'total_rooms': stats['total_rooms'],
        'success_rate': (stats['successful_extractions'] / stats['total_hotels'] * 100) if stats['total_hotels'] > 0 else 0,
        'consolidation_file': stats.get('consolidation_file'),
        'preview_data': stats.get('preview_data', []),
        'unique_columns': len(stats.get('unique_headers', [])),
        'consolidation_date': stats.get('consolidation_date')
    }
    
    return summary


def create_base_hotel_row(hotel_name: str, hotel_address: str, result: Dict) -> Dict[str, Any]:
    """Crée une ligne de base avec les métadonnées d'hôtel
    
    Args:
        hotel_name (str): Nom de l'hôtel
        hotel_address (str): Adresse de l'hôtel
        result (Dict): Résultat d'extraction
        
    Returns:
        Dict[str, Any]: Ligne de base avec métadonnées
    """
    
    return {
        'hotel_name': hotel_name,
        'hotel_address': hotel_address,
        'extraction_date': result.get('extraction_date', ''),
        'cvent_url': result.get('url', ''),
        'cvent_interface_type': result.get('cvent_data', {}).get('interface_type', '') if result.get('cvent_data') else '',
    }


def add_gmaps_data_to_row(row: Dict[str, Any], gmaps_data: Dict[str, Any], stats: Dict[str, Any]):
    """Ajoute les données Google Maps à une ligne
    
    Args:
        row (Dict[str, Any]): Ligne à enrichir
        gmaps_data (Dict[str, Any]): Données Google Maps
        stats (Dict[str, Any]): Statistiques pour tracking des headers
    """
    
    # Mapping des données Google Maps avec préfixe pour éviter les conflits
    gmaps_mapping = {
        'gmaps_sharable_link': gmaps_data.get('sharableLink', ''),
        'gmaps_name': gmaps_data.get('name', ''),
        'gmaps_is_closed': gmaps_data.get('isClosed', False),
        'gmaps_website': gmaps_data.get('website', ''),
        'gmaps_category': gmaps_data.get('category', ''),
        'gmaps_address': gmaps_data.get('address', ''),
        'gmaps_region': gmaps_data.get('oloc', ''),
        'gmaps_rating': gmaps_data.get('averageRating', 0),
        'gmaps_review_count': gmaps_data.get('reviewCount', 0),
        'gmaps_phone': gmaps_data.get('phoneNumber', ''),
        'gmaps_image_url': gmaps_data.get('headerImageUrl', ''),
        'gmaps_opening_hours': gmaps_data.get('openingHours', ''),
    }
    
    # Ajouter toutes les données Google Maps
    for key, value in gmaps_mapping.items():
        row[key] = value
        stats['unique_headers'].add(key)


def add_website_data_to_row(row: Dict[str, Any], website_data: Dict[str, Any], stats: Dict[str, Any]):
    """Ajoute les données Website à une ligne
    
    Args:
        row (Dict[str, Any]): Ligne à enrichir
        website_data (Dict[str, Any]): Données Website
        stats (Dict[str, Any]): Statistiques pour tracking des headers
    """
    
    # 🔥 NOUVEAU: Support données Firecrawl enrichies + Legacy
    website_mapping = {
        # Métadonnées du site (Firecrawl + Legacy)
        'website_url': website_data.get('website_url', ''),
        'website_source': website_data.get('website_source', ''),
        'website_title': website_data.get('website_title', website_data.get('hotel_website_title', '')),
        'website_description': website_data.get('website_description', ''),
        
        # 🔥 Nouvelles données Firecrawl
        'website_phone': website_data.get('hotel_phone', ''),
        'website_email': website_data.get('hotel_email', ''),
        'website_opening_hours': website_data.get('opening_hours', ''),
        'website_price_range': website_data.get('price_range', ''),
        
        # Photos
        'website_photos_urls': ';'.join(website_data.get('photos_urls', [])),
        'website_photos_count': website_data.get('photos_count', 0),
        
        # Données structurées extraites par LLM
        'website_capacite_max': website_data.get('capacite_max'),
        'website_nombre_chambre': website_data.get('nombre_chambre'),
        'website_nombre_chambre_twin': website_data.get('nombre_chambre_twin'),
        'website_nombre_etoile': website_data.get('nombre_etoile'),
        'website_pr_amphi': website_data.get('pr_amphi'),
        'website_pr_hotel': website_data.get('pr_hotel'),
        'website_pr_acces_facile': website_data.get('pr_acces_facile'),
        'website_pr_banquet': website_data.get('pr_banquet'),
        'website_pr_contact': website_data.get('pr_contact'),
        'website_pr_room_nb': website_data.get('pr_room_nb'),
        'website_pr_lieu_atypique': website_data.get('pr_lieu_atypique'),
        'website_pr_nature': website_data.get('pr_nature'),
        'website_pr_mer': website_data.get('pr_mer'),
        'website_pr_montagne': website_data.get('pr_montagne'),
        'website_pr_centre_ville': website_data.get('pr_centre_ville'),
        'website_pr_parking': website_data.get('pr_parking'),
        'website_pr_restaurant': website_data.get('pr_restaurant'),
        'website_pr_piscine': website_data.get('pr_piscine'),
        'website_pr_spa': website_data.get('pr_spa'),
        'website_pr_wifi': website_data.get('pr_wifi'),
        'website_pr_sun': website_data.get('pr_sun'),
        'website_pr_contemporaine': website_data.get('pr_contemporaine'),
        'website_pr_acces_pmr': website_data.get('pr_acces_pmr'),
        'website_pr_visio': website_data.get('pr_visio'),
        'website_pr_eco_label': website_data.get('pr_eco_label'),
        'website_pr_rooftop': website_data.get('pr_rooftop'),
        'website_pr_esat': website_data.get('pr_esat'),
        'website_summary': website_data.get('summary', ''),
        
        # 🔥 Nouvelles données salles de réunion Firecrawl
        'website_meeting_rooms_available': website_data.get('meeting_rooms_available'),
        'website_meeting_rooms_count': website_data.get('meeting_rooms_count'),
        'website_largest_room_capacity': website_data.get('largest_room_capacity'),
        
        # Métadonnées d'extraction (Legacy + Firecrawl)
        'website_content_length': website_data.get('content_length', 0),
        'website_images_found': website_data.get('images_found', 0),
        'website_fields_extracted': website_data.get('llm_fields_extracted', 0),
        'website_extraction_method': website_data.get('extraction_method', 'unknown'),
    }
    
    # Ajouter toutes les données Website
    for key, value in website_mapping.items():
        row[key] = value
        stats['unique_headers'].add(key)


def organize_columns_and_clean_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Réorganise les colonnes et nettoie les doublons selon l'ordre souhaité
    
    Args:
        df (pd.DataFrame): DataFrame à réorganiser
        
    Returns:
        pd.DataFrame: DataFrame avec colonnes réorganisées et doublons supprimés
    """
    
    available_cols = df.columns.tolist()
    
    # 🔧 GESTION DES DOUBLONS - Nettoyer toutes les variantes
    duplicate_patterns = {
        'capacite_maximum': ['capacité_max', 'capacité_maximale', 'capacite_max'],
        'salle_taille': ['taille'],
        'capacite_cabaret': ['demi_lune']
    }
    
    # Supprimer les colonnes dupliquées
    for preferred, duplicates in duplicate_patterns.items():
        for duplicate in duplicates:
            if preferred in available_cols and duplicate in available_cols:
                print(f"🔧 Suppression colonne dupliquée: '{duplicate}' (conservé: '{preferred}')")
                df = df.drop(columns=[duplicate])
                available_cols.remove(duplicate)
    
    # 🎯 SUPPRESSION SPÉCIALE: website_capacite_max (doublon avec Cvent)
    if 'website_capacite_max' in available_cols and 'capacite_maximum' in available_cols:
        # Priorité aux données Cvent (plus précises)
        print(f"🔧 Suppression website_capacite_max (doublon avec capacite_maximum Cvent)")
        df = df.drop(columns=['website_capacite_max'])
        available_cols.remove('website_capacite_max')
    
    # 🔥 SUPPRESSION COLONNES .1 (créées par Pandas lors de doublons)
    dot_one_columns = [col for col in available_cols if col.endswith('.1')]
    if dot_one_columns:
        print(f"🔧 Suppression colonnes .1 (doublons Pandas): {dot_one_columns}")
        df = df.drop(columns=dot_one_columns)
        for col in dot_one_columns:
            available_cols.remove(col)
    
    # 🎯 ORDRE SOUHAITÉ DES COLONNES
    
    # 1. Métadonnées hôtel
    hotel_cols = ['hotel_name', 'hotel_address', 'extraction_date', 'cvent_interface_type', 'cvent_url']
    
    # 2. Informations salle principales
    salle_cols = ['salle_nom', 'hauteur_plafond', 'salle_taille', 'dimensions']
    
    # 3. Capacités (toutes les capacités ensemble)
    capacite_cols = [col for col in available_cols if 'capacit' in col.lower()]
    
    # 4. Autres données Cvent
    other_cvent_cols = [col for col in available_cols 
                       if col not in hotel_cols + salle_cols + capacite_cols 
                       and not col.startswith('gmaps_') 
                       and not col.startswith('website_')]
    
    # 5. Google Maps
    gmaps_cols = [col for col in available_cols if col.startswith('gmaps_')]
    
    # 6. Website données (à la fin)
    website_cols = [col for col in available_cols if col.startswith('website_')]
    
    # Construire l'ordre final
    ordered_cols = []
    
    # Ajouter chaque groupe s'il existe
    for group in [hotel_cols, salle_cols, capacite_cols, other_cvent_cols, gmaps_cols, website_cols]:
        for col in group:
            if col in available_cols and col not in ordered_cols:
                ordered_cols.append(col)
    
    # Ajouter toute colonne oubliée (sécurité)
    for col in available_cols:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    print(f"🔧 Colonnes réorganisées: {len(ordered_cols)} colonnes")
    print(f"   📋 Salles: {len([c for c in salle_cols if c in available_cols])}")
    print(f"   💪 Capacités: {len(capacite_cols)} (dupliquées supprimées)")
    print(f"   🗺️ Google Maps: {len(gmaps_cols)}")
    print(f"   🌐 Website: {len(website_cols)}")
    
    # Debug: Afficher les colonnes de capacité finales
    final_capacite_cols = [col for col in ordered_cols if 'capacit' in col.lower()]
    print(f"   🎯 Capacités finales: {final_capacite_cols}")
    
    # Réorganiser le DataFrame
    return df[ordered_cols]


def clean_data_types_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les types de données pour éviter les erreurs PyArrow dans Streamlit
    
    Args:
        df (pd.DataFrame): DataFrame à nettoyer
        
    Returns:
        pd.DataFrame: DataFrame avec types nettoyés
    """
    
    # Colonnes de capacité/numérique qui peuvent contenir des "-"
    numeric_columns = [col for col in df.columns if any(keyword in col.lower() 
                      for keyword in ['capacit', 'hauteur', 'taille', 'rating', 'review'])]
    
    for col in numeric_columns:
        if col in df.columns:
            try:
                # Remplacer "-" et valeurs vides par NaN pour les colonnes numériques
                df[col] = df[col].replace(['-', '', 'nan', 'NaN'], pd.NA)
                
                # Tenter conversion en numérique, garder comme string si échec
                df[col] = pd.to_numeric(df[col], errors='ignore')
                
            except Exception as e:
                print(f"⚠️ Nettoyage type {col}: {e}")
                # En cas d'erreur, forcer en string
                df[col] = df[col].astype(str)
    
    print(f"🔧 Types nettoyés pour {len(numeric_columns)} colonnes numériques")
    
    # Debug final: Vérifier qu'il n'y a plus de doublons
    capacity_cols = [col for col in df.columns if 'capacit' in col.lower()]
    dot_one_remaining = [col for col in df.columns if col.endswith('.1')]
    
    print(f"📊 Colonnes capacité finales: {capacity_cols}")
    if dot_one_remaining:
        print(f"⚠️ ATTENTION: Colonnes .1 restantes: {dot_one_remaining}")
    else:
        print("✅ Aucune colonne .1 restante")
    
    return df


def load_csv_preview(csv_path: str, max_rows: int = 10) -> pd.DataFrame:
    """Charge un aperçu d'un fichier CSV
    
    Args:
        csv_path (str): Chemin vers le fichier CSV
        max_rows (int): Nombre maximum de lignes à charger
        
    Returns:
        pd.DataFrame: Aperçu des données
    """
    
    try:
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path, nrows=max_rows, encoding='utf-8')
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Erreur lecture aperçu {csv_path}: {e}")
        return pd.DataFrame() 