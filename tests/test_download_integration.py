"""
Test d'intégration pour la fonctionnalité de téléchargement CSV
Teste la fonctionnalité complète dans un contexte réaliste
"""

import pytest
import pandas as pd
import asyncio
from pathlib import Path
from datetime import datetime

# Import des modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError
from modules.database_service import DatabaseService
from modules.parallel_processor_db import ParallelHotelProcessorDB, ParallelConfig
from services.extraction_service_db import ExtractionServiceDB


class TestDownloadIntegration:
    """Tests d'intégration pour le téléchargement CSV"""

    @pytest.fixture
    def sample_hotels_data(self):
        """Données d'hôtels pour tests"""
        return [
            {'name': 'Hotel Download Test 1', 'address': 'Brussels, BE', 'url': 'http://test1.com'},
            {'name': 'Hotel Download Test 2', 'address': 'Paris, FR', 'url': 'http://test2.com'},
            {'name': 'Hotel Download Test 3', 'address': 'Amsterdam, NL', 'url': 'http://test3.com'}
        ]

    @pytest.fixture
    def db_service(self):
        """Service de base de données"""
        try:
            return DatabaseService()
        except SupabaseError as e:
            pytest.skip(f"Supabase non configuré: {e}")

    def test_csv_download_workflow(self, db_service, sample_hotels_data):
        """Test du workflow complet avec téléchargement CSV"""
        print("\n🚀 Test workflow complet avec téléchargement CSV")

        # 1. Créer session
        session_id = db_service.create_new_session("test_download.csv", len(sample_hotels_data))
        print(f"✅ Session créée: {session_id[:8]}...")

        # 2. Préparer hotels dans la DB
        hotel_ids = db_service.prepare_hotels_batch(session_id, sample_hotels_data)
        print(f"✅ {len(hotel_ids)} hôtels préparés")

        # 3. Simuler extractions partielles (seulement 2 sur 3)
        for i, hotel_id in enumerate(hotel_ids[:2]):  # Seulement les 2 premiers
            cvent_result = {
                'success': True,
                'data': {
                    'interface_type': 'grid_direct',
                    'headers': ['Salles de réunion', 'Taille', 'Théâtre', 'En banquet'],
                    'rows': [
                        [f'Salle Executive {i+1}', f'{40+i*10} m²', f'{30+i*5}', f'{25+i*5}'],
                        [f'Salle Board {i+1}', f'{25+i*5} m²', f'{20+i*3}', f'{20+i*3}']
                    ]
                }
            }

            success = db_service.process_hotel_extraction(
                hotel_id=hotel_id,
                cvent_result=cvent_result
            )
            assert success
            print(f"✅ Hôtel {i+1} extrait avec succès")

        # 4. Le 3ème hôtel reste "pending" (simulation d'interruption)
        print("⏸️ Simulation d'interruption après 2 hôtels")

        # 5. Vérifier les statistiques partielles
        export_stats = db_service.get_session_export_stats(session_id)
        print(f"📊 Stats avant export: {export_stats}")

        assert export_stats['total_hotels'] == 3
        assert export_stats['hotels_with_data'] == 2
        assert export_stats['total_rooms'] == 4  # 2 hôtels × 2 salles
        assert export_stats['export_ready'] is True

        # 6. Générer CSV partiel
        print("📥 Génération du CSV partiel...")
        csv_content = db_service.export_session_to_csv(
            session_id=session_id,
            include_empty_rooms=True
        )

        assert csv_content is not None
        assert len(csv_content) > 0

        # 7. Vérifier contenu du CSV
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        print(f"📄 CSV généré: {len(df)} lignes")

        # Doit contenir 5 lignes : 2 hôtels × 2 salles + 1 hôtel vide
        assert len(df) == 5

        # Vérifier les hôtels extraits
        extracted_hotels = df[df['extraction_status'] == 'completed']
        assert len(extracted_hotels) == 4  # 2 hôtels × 2 salles

        # Vérifier l'hôtel non extrait
        pending_hotels = df[df['extraction_status'] == 'pending']
        assert len(pending_hotels) == 1

        # Vérifier les noms d'hôtels
        hotel_names = df['hotel_name'].unique()
        assert 'Hotel Download Test 1' in hotel_names
        assert 'Hotel Download Test 2' in hotel_names
        assert 'Hotel Download Test 3' in hotel_names

        # Vérifier les salles extraites (pas d'espaces vides ou NaN)
        extracted_rooms = df[df['nom_salle'].notna() & (df['nom_salle'] != '')]
        assert len(extracted_rooms) == 4

        salles = extracted_rooms['nom_salle'].tolist()
        assert 'Salle Executive 1' in salles
        assert 'Salle Board 1' in salles
        assert 'Salle Executive 2' in salles
        assert 'Salle Board 2' in salles

        print("✅ Contenu CSV validé")

        # 8. Test de sauvegarde fichier
        filename = f"test_interruption_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f"💾 Test sauvegarde: {filename}")

        # Simuler la sauvegarde
        assert len(csv_content) > 500  # Un minimum de contenu
        assert csv_content.startswith('hotel_name,')  # Headers CSV

        # Nettoyer
        db_service.finalize_session(session_id, success=False)
        print("🧹 Session nettoyée")

        print("🎉 Test workflow téléchargement CSV RÉUSSI")

    def test_csv_download_empty_session_scenario(self, db_service):
        """Test téléchargement sur une session juste démarrée (0 extraction)"""
        print("\n📭 Test téléchargement session vide")

        # Créer session sans extraction
        session_id = db_service.create_new_session("test_empty_dl.csv", 5)

        # Préparer hôtels mais sans extraction
        hotels_data = [{'name': f'Hotel Empty {i}', 'address': f'City {i}', 'url': f'http://empty{i}.com'} for i in range(5)]
        hotel_ids = db_service.prepare_hotels_batch(session_id, hotels_data)

        # Pas d'extraction - tous restent "pending"

        # Tenter export
        export_stats = db_service.get_session_export_stats(session_id)
        print(f"📊 Stats session vide: {export_stats}")

        assert export_stats['total_hotels'] == 5
        assert export_stats['hotels_with_data'] == 0
        assert export_stats['total_rooms'] == 0
        assert export_stats['export_ready'] is False

        # CSV doit quand même être généré avec hôtels vides
        csv_content = db_service.export_session_to_csv(
            session_id=session_id,
            include_empty_rooms=True
        )

        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        assert len(df) == 5  # 5 hôtels sans salles
        assert all(df['nom_salle'].isna())  # Pas de salles (NaN au lieu de vide)
        assert all(df['extraction_status'] == 'pending')  # Tous pending

        print("✅ Export session vide OK")

    def test_csv_download_error_recovery(self, db_service, sample_hotels_data):
        """Test récupération après erreur lors d'extraction"""
        print("\n🔧 Test récupération après erreur")

        session_id = db_service.create_new_session("test_error.csv", len(sample_hotels_data))
        hotel_ids = db_service.prepare_hotels_batch(session_id, sample_hotels_data)

        # Simuler succès puis échec
        # Premier hôtel : succès
        cvent_success = {
            'success': True,
            'data': {
                'interface_type': 'grid_direct',
                'headers': ['Salles de réunion', 'Théâtre'],
                'rows': [['Salle Success', '50']]
            }
        }

        db_service.process_hotel_extraction(
            hotel_id=hotel_ids[0],
            cvent_result=cvent_success
        )

        # Deuxième hôtel : échec simulé - marquer directement comme échoué
        db_service.client.update_hotel_status(
            hotel_id=hotel_ids[1],
            status="failed",
            error_message="Simulation échec"
        )

        # Le CSV doit contenir les données partielles
        csv_content = db_service.export_session_to_csv(session_id, include_empty_rooms=True)

        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        # Vérifier mix de status
        statuses = df['extraction_status'].unique()
        assert 'completed' in statuses
        assert 'failed' in statuses
        assert 'pending' in statuses

        # Vérifier données du succès
        success_rows = df[df['extraction_status'] == 'completed']
        assert len(success_rows) == 1
        assert success_rows.iloc[0]['nom_salle'] == 'Salle Success'

        print("✅ Récupération après erreur OK")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])