"""
Gestionnaire de résultats - Consolidation et export des données extraites
"""

import json
import csv
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path

from config.settings import settings


class ResultsManager:
    """Gestionnaire pour la consolidation et l'export des résultats"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.consolidated_data = []
        self.metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'total_hotels': 0,
            'successful_extractions': 0,
            'data_sources': []
        }
    
    def consolidate_results(self, extraction_results: List[Dict[str, Any]],
                          include_cvent: bool = True,
                          include_gmaps: bool = True, 
                          include_website: bool = True) -> List[Dict[str, Any]]:
        """
        Consolide les résultats d'extraction en format unifié
        
        Args:
            extraction_results: Résultats bruts d'extraction
            include_cvent: Inclure les données Cvent
            include_gmaps: Inclure les données Google Maps
            include_website: Inclure les données website
            
        Returns:
            Liste des données consolidées
        """
        print(f"📋 Consolidation de {len(extraction_results)} résultats...")
        
        consolidated = []
        successful_count = 0
        
        for result in extraction_results:
            try:
                # Données de base de l'hôtel
                hotel_base = result.get('hotel_data', {})
                consolidated_hotel = {
                    # Informations de base
                    'name': hotel_base.get('name', ''),
                    'address': hotel_base.get('address', ''),
                    'city': hotel_base.get('city', ''),
                    'country': hotel_base.get('country', ''),
                    
                    # Métadonnées de traitement
                    'extraction_success': result.get('success', False),
                    'processing_time': result.get('processing_time', 0),
                    'extraction_timestamp': result.get('timestamp', ''),
                    'errors': '; '.join(result.get('errors', [])),
                    
                    # Initialiser champs optionnels
                    'phone': '',
                    'email': '',
                    'website': '',
                    'rating': '',
                    'reviews_count': '',
                    'meeting_rooms_count': 0,
                    'meeting_rooms_details': '',
                    'facilities': '',
                    'description': ''
                }
                
                # Consolidation données Cvent
                if include_cvent and result.get('cvent_data'):
                    cvent_data = result['cvent_data']
                    if cvent_data.get('success'):
                        consolidated_hotel.update({
                            'meeting_rooms_count': len(cvent_data.get('meeting_rooms', [])),
                            'meeting_rooms_details': self._format_meeting_rooms(cvent_data.get('meeting_rooms', [])),
                            'cvent_venue_id': cvent_data.get('venue_id', ''),
                            'cvent_extraction_success': True
                        })
                    else:
                        consolidated_hotel['cvent_extraction_success'] = False
                        consolidated_hotel['cvent_error'] = cvent_data.get('error', '')
                
                # Consolidation données Google Maps
                if include_gmaps and result.get('gmaps_data'):
                    gmaps_data = result['gmaps_data']
                    if gmaps_data.get('success'):
                        consolidated_hotel.update({
                            'phone': gmaps_data.get('phone', ''),
                            'website': gmaps_data.get('website', ''),
                            'rating': gmaps_data.get('rating', ''),
                            'reviews_count': gmaps_data.get('reviews_count', ''),
                            'gmaps_place_id': gmaps_data.get('place_id', ''),
                            'gmaps_extraction_success': True
                        })
                    else:
                        consolidated_hotel['gmaps_extraction_success'] = False
                        consolidated_hotel['gmaps_error'] = gmaps_data.get('error', '')
                
                # Consolidation données Website
                if include_website and result.get('website_data'):
                    website_data = result['website_data']
                    if website_data.get('success'):
                        consolidated_hotel.update({
                            'description': website_data.get('description', ''),
                            'facilities': website_data.get('facilities', ''),
                            'email': website_data.get('email', ''),
                            'website_content_length': len(website_data.get('raw_content', '')),
                            'website_extraction_success': True
                        })
                        
                        # Mettre à jour website si pas trouvé via GMaps
                        if not consolidated_hotel['website'] and website_data.get('website_url'):
                            consolidated_hotel['website'] = website_data['website_url']
                    else:
                        consolidated_hotel['website_extraction_success'] = False
                        consolidated_hotel['website_error'] = website_data.get('error', '')
                
                consolidated.append(consolidated_hotel)
                
                if consolidated_hotel['extraction_success']:
                    successful_count += 1
                    
            except Exception as e:
                print(f"❌ Erreur consolidation pour {hotel_base.get('name', 'Unknown')}: {e}")
                # Ajouter entrée d'erreur
                consolidated.append({
                    'name': hotel_base.get('name', 'Unknown'),
                    'extraction_success': False,
                    'errors': f'Erreur consolidation: {str(e)}'
                })
        
        # Mettre à jour métadonnées
        self.metadata.update({
            'total_hotels': len(consolidated),
            'successful_extractions': successful_count,
            'data_sources': [
                source for source, enabled in [
                    ('cvent', include_cvent),
                    ('gmaps', include_gmaps), 
                    ('website', include_website)
                ] if enabled
            ]
        })
        
        self.consolidated_data = consolidated
        print(f"✅ Consolidation terminée: {successful_count}/{len(consolidated)} succès")
        
        return consolidated
    
    def export_to_csv(self, filename: Optional[str] = None,
                     include_metadata: bool = False,
                     streaming: bool = False) -> str:
        """
        Exporte les données consolidées en CSV
        
        Args:
            filename: Nom du fichier (auto-généré si None)
            include_metadata: Inclure fichier métadonnées JSON
            streaming: Utiliser export streaming pour gros volumes
            
        Returns:
            Chemin du fichier CSV créé
        """
        if not self.consolidated_data:
            raise ValueError("Aucune donnée consolidée à exporter. Appelez consolidate_results() d'abord.")
        
        # Générer nom de fichier si nécessaire
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hotels_extraction_{timestamp}.csv"
        
        csv_path = self.output_dir / filename
        
        # Choisir méthode d'export selon le volume
        if streaming or len(self.consolidated_data) > 1000:
            self._export_csv_streaming(csv_path)
            print(f"📄 CSV exporté (streaming): {csv_path} - {len(self.consolidated_data)} entrées")
        else:
            # Export classique pour petits volumes
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                if self.consolidated_data:
                    fieldnames = self.consolidated_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(self.consolidated_data)
            print(f"📄 CSV exporté: {csv_path} - {len(self.consolidated_data)} entrées")
        
        # Exporter métadonnées si demandé
        if include_metadata:
            metadata_path = csv_path.with_suffix('.metadata.json')
            with open(metadata_path, 'w', encoding='utf-8') as metafile:
                json.dump(self.metadata, metafile, indent=2, ensure_ascii=False)
            print(f"📋 Métadonnées exportées: {metadata_path}")
        
        return str(csv_path)
    
    def export_to_excel(self, filename: Optional[str] = None,
                       include_summary_sheet: bool = True) -> str:
        """
        Exporte les données consolidées en Excel avec feuilles multiples
        
        Args:
            filename: Nom du fichier (auto-généré si None)
            include_summary_sheet: Inclure feuille de résumé
            
        Returns:
            Chemin du fichier Excel créé
        """
        if not self.consolidated_data:
            raise ValueError("Aucune donnée consolidée à exporter.")
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hotels_extraction_{timestamp}.xlsx"
        
        excel_path = self.output_dir / filename
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Feuille principale avec données
            df_main = pd.DataFrame(self.consolidated_data)
            df_main.to_excel(writer, sheet_name='Hotels Data', index=False)
            
            # Feuille de résumé si demandée
            if include_summary_sheet:
                summary_data = self._create_summary_data()
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Feuille métadonnées
            metadata_rows = []
            for key, value in self.metadata.items():
                metadata_rows.append({'Metric': key, 'Value': str(value)})
            df_metadata = pd.DataFrame(metadata_rows)
            df_metadata.to_excel(writer, sheet_name='Metadata', index=False)
        
        print(f"📊 Excel exporté: {excel_path}")
        return str(excel_path)
    
    def _format_meeting_rooms(self, meeting_rooms: List[Dict[str, Any]]) -> str:
        """Formate les salles de réunion pour export CSV"""
        if not meeting_rooms:
            return ""
        
        formatted_rooms = []
        for room in meeting_rooms:
            room_info = f"{room.get('name', 'N/A')}"
            if room.get('capacity'):
                room_info += f" ({room['capacity']} pers.)"
            if room.get('size'):
                room_info += f" - {room['size']}"
            formatted_rooms.append(room_info)
        
        return " | ".join(formatted_rooms)
    
    def _create_summary_data(self) -> List[Dict[str, Any]]:
        """Crée les données de résumé pour Excel"""
        if not self.consolidated_data:
            return []
        
        df = pd.DataFrame(self.consolidated_data)
        
        summary = [
            {'Metric': 'Total Hotels', 'Value': len(df)},
            {'Metric': 'Successful Extractions', 'Value': df['extraction_success'].sum()},
            {'Metric': 'Success Rate', 'Value': f"{df['extraction_success'].mean() * 100:.1f}%"},
            {'Metric': 'Avg Processing Time', 'Value': f"{df['processing_time'].mean():.2f}s"},
            {'Metric': 'Hotels with Phone', 'Value': df['phone'].notna().sum()},
            {'Metric': 'Hotels with Website', 'Value': df['website'].notna().sum()},
            {'Metric': 'Hotels with Meeting Rooms', 'Value': (df['meeting_rooms_count'] > 0).sum()},
            {'Metric': 'Total Meeting Rooms', 'Value': df['meeting_rooms_count'].sum()},
        ]
        
        return summary
    
    def _export_csv_streaming(self, csv_path: Path):
        """
        Export CSV en streaming pour gérer de gros volumes sans surcharge mémoire
        Traite les données par chunks pour optimiser la mémoire
        """
        if not self.consolidated_data:
            return
        
        chunk_size = 100  # Traiter par chunks de 100 entrées
        total_entries = len(self.consolidated_data)
        chunks_count = (total_entries + chunk_size - 1) // chunk_size
        
        print(f"📊 Export streaming: {total_entries} entrées en {chunks_count} chunks")
        
        # Déterminer les champs à partir de la première entrée
        fieldnames = list(self.consolidated_data[0].keys()) if self.consolidated_data else []
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            
            # Traiter par chunks
            for chunk_num in range(chunks_count):
                start_idx = chunk_num * chunk_size
                end_idx = min(start_idx + chunk_size, total_entries)
                
                chunk_data = self.consolidated_data[start_idx:end_idx]
                
                # Écrire le chunk
                for row in chunk_data:
                    # Nettoyer les données problématiques pour CSV
                    cleaned_row = self._clean_csv_row(row)
                    writer.writerow(cleaned_row)
                
                # Flush périodique pour éviter accumulation en mémoire
                csvfile.flush()
                
                # Progress pour gros volumes
                if chunks_count > 10:
                    progress = ((chunk_num + 1) / chunks_count) * 100
                    print(f"   📈 Progress: {progress:.1f}% ({end_idx}/{total_entries})")
        
        print(f"✅ Export streaming terminé: {total_entries} entrées")
    
    def _clean_csv_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Nettoie une ligne de données pour export CSV
        Gère les caractères problématiques et types non-sérialisables
        """
        cleaned_row = {}
        
        for key, value in row.items():
            if value is None:
                cleaned_row[key] = ''
            elif isinstance(value, (list, dict)):
                # Convertir structures complexes en string
                cleaned_row[key] = str(value)
            elif isinstance(value, str):
                # Nettoyer caractères problématiques
                cleaned_value = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                # Limiter longueur pour éviter cellules énormes
                if len(cleaned_value) > 1000:
                    cleaned_value = cleaned_value[:997] + "..."
                cleaned_row[key] = cleaned_value
            else:
                cleaned_row[key] = str(value)
        
        return cleaned_row
    
    async def export_to_csv_async(self, filename: Optional[str] = None,
                                include_metadata: bool = False,
                                chunk_size: int = 100) -> str:
        """
        Version asynchrone de l'export CSV pour très gros volumes
        Permet de ne pas bloquer l'event loop
        """
        if not self.consolidated_data:
            raise ValueError("Aucune donnée consolidée à exporter.")
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hotels_extraction_{timestamp}.csv"
        
        csv_path = self.output_dir / filename
        
        print(f"📊 Export CSV asynchrone: {len(self.consolidated_data)} entrées")
        
        fieldnames = list(self.consolidated_data[0].keys()) if self.consolidated_data else []
        
        # Ouvrir en mode asynchrone pour très gros volumes
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            
            # Traiter par chunks avec yield entre les chunks
            total_entries = len(self.consolidated_data)
            chunks_count = (total_entries + chunk_size - 1) // chunk_size
            
            for chunk_num in range(chunks_count):
                start_idx = chunk_num * chunk_size
                end_idx = min(start_idx + chunk_size, total_entries)
                
                chunk_data = self.consolidated_data[start_idx:end_idx]
                
                # Écrire le chunk
                for row in chunk_data:
                    cleaned_row = self._clean_csv_row(row)
                    writer.writerow(cleaned_row)
                
                csvfile.flush()
                
                # Yield control pour ne pas bloquer l'event loop
                await asyncio.sleep(0.001)  # Micro-pause
                
                if chunks_count > 20:
                    progress = ((chunk_num + 1) / chunks_count) * 100
                    print(f"   📈 Async Progress: {progress:.1f}%")
        
        # Métadonnées
        if include_metadata:
            metadata_path = csv_path.with_suffix('.metadata.json')
            with open(metadata_path, 'w', encoding='utf-8') as metafile:
                json.dump(self.metadata, metafile, indent=2, ensure_ascii=False)
        
        print(f"✅ Export CSV asynchrone terminé: {csv_path}")
        return str(csv_path)
    
    def get_consolidation_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de consolidation"""
        if not self.consolidated_data:
            return {'error': 'No consolidated data available'}
        
        df = pd.DataFrame(self.consolidated_data)
        
        return {
            'total_hotels': len(df),
            'successful_extractions': int(df['extraction_success'].sum()),
            'success_rate': f"{df['extraction_success'].mean() * 100:.1f}%",
            'data_completeness': {
                'phone': f"{(df['phone'] != '').sum()}/{len(df)}",
                'email': f"{(df['email'] != '').sum()}/{len(df)}",
                'website': f"{(df['website'] != '').sum()}/{len(df)}",
                'meeting_rooms': f"{(df['meeting_rooms_count'] > 0).sum()}/{len(df)}"
            },
            'average_processing_time': f"{df['processing_time'].mean():.2f}s"
        }