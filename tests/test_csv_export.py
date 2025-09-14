"""
Tests pour la fonctionnalité d'export CSV des données partielles
Teste l'export depuis Supabase avec différents scénarios
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

# Import des modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError
from modules.database_service import DatabaseService


class TestCSVExport:
    """Tests pour l'export CSV depuis Supabase"""

    @pytest.fixture
    def db_service(self):
        """Service de base de données configuré"""
        try:
            return DatabaseService()
        except SupabaseError as e:
            pytest.skip(f"Supabase non configuré: {e}")

    @pytest.fixture
    def test_session_with_data(self, db_service):
        """Crée une session de test avec des données"""
        # Créer session
        session_id = db_service.create_new_session("test_export.csv", 2)

        # Préparer données test
        hotels_data = [
            {'name': 'Hotel Test 1', 'address': 'Bruxelles, BE', 'url': 'http://test1.com'},
            {'name': 'Hotel Test 2', 'address': 'Paris, FR', 'url': 'http://test2.com'}
        ]

        # Insérer hôtels
        hotel_ids = db_service.prepare_hotels_batch(session_id, hotels_data)

        # Simuler extraction réussie sur le premier hôtel
        if hotel_ids:
            # Données Cvent simulées
            cvent_result = {
                'success': True,
                'data': {
                    'interface_type': 'grid_direct',
                    'headers': ['Salles de réunion', 'Taille', 'En U', 'Théâtre'],
                    'rows': [
                        ['Salle Apollo', '50 m²', '20', '40'],
                        ['Salle Jupiter', '30 m²', '15', '25']
                    ]
                }
            }

            # Traiter l'extraction du premier hôtel
            db_service.process_hotel_extraction(
                hotel_id=hotel_ids[0],
                cvent_result=cvent_result
            )

        return session_id, hotel_ids

    def test_export_session_empty(self, db_service):
        """Test export d'une session vide"""
        # Créer session sans données
        session_id = db_service.create_new_session("test_empty.csv", 0)

        csv_content = db_service.export_session_to_csv(session_id)

        # Doit retourner un CSV avec headers seulement
        assert csv_content is not None
        assert len(csv_content) > 0

        # Vérifier qu'il y a les headers
        lines = csv_content.strip().split('\n')
        assert len(lines) >= 1  # Au moins la ligne de header

        headers = lines[0].split(',')
        expected_headers = ['hotel_name', 'nom_salle', 'surface', 'capacite_theatre']
        for header in expected_headers:
            assert header in headers

        print("✅ Export CSV vide OK")

    def test_export_session_with_data(self, db_service, test_session_with_data):
        """Test export d'une session avec des données"""
        session_id, hotel_ids = test_session_with_data

        # Exporter les données
        csv_content = db_service.export_session_to_csv(
            session_id=session_id,
            include_empty_rooms=True
        )

        assert csv_content is not None
        assert len(csv_content) > 0

        # Parser le CSV pour vérification
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        # Doit avoir au moins les données du premier hôtel (2 salles)
        assert len(df) >= 2

        # Vérifier les données du premier hôtel
        hotel_rows = df[df['hotel_name'] == 'Hotel Test 1']
        assert len(hotel_rows) >= 2  # 2 salles extraites

        # Vérifier les salles
        salles = hotel_rows['nom_salle'].tolist()
        assert 'Salle Apollo' in salles
        assert 'Salle Jupiter' in salles

        # Vérifier les capacités
        apollo_row = hotel_rows[hotel_rows['nom_salle'] == 'Salle Apollo'].iloc[0]
        assert apollo_row['surface'] == '50 m²'
        assert str(apollo_row['capacite_u']) == '20' or apollo_row['capacite_u'] == 20
        assert str(apollo_row['capacite_theatre']) == '40' or apollo_row['capacite_theatre'] == 40

        print(f"✅ Export CSV avec données OK: {len(df)} lignes")

    def test_export_stats(self, db_service, test_session_with_data):
        """Test des statistiques d'export"""
        session_id, hotel_ids = test_session_with_data

        stats = db_service.get_session_export_stats(session_id)

        assert stats is not None
        assert stats['session_id'] == session_id
        assert stats['total_hotels'] == 2  # 2 hôtels insérés
        assert stats['hotels_with_data'] >= 1  # Au moins 1 avec extraction
        assert stats['total_rooms'] >= 2  # Au moins 2 salles
        assert stats['export_ready'] is True

        print(f"✅ Stats export OK: {stats}")

    def test_export_only_rooms(self, db_service, test_session_with_data):
        """Test export sans inclure les hôtels vides"""
        session_id, hotel_ids = test_session_with_data

        # Export sans hôtels vides
        csv_content = db_service.export_session_to_csv(
            session_id=session_id,
            include_empty_rooms=False
        )

        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        # Doit contenir seulement l'hôtel avec des salles
        hotel_names = df['hotel_name'].unique()
        assert len(hotel_names) == 1
        assert 'Hotel Test 1' in hotel_names
        assert 'Hotel Test 2' not in hotel_names  # Pas de salles donc exclu

        print(f"✅ Export salles seulement OK: {len(df)} lignes")

    def test_csv_format_consistency(self, db_service, test_session_with_data):
        """Test cohérence du format CSV"""
        session_id, hotel_ids = test_session_with_data

        csv_content = db_service.export_session_to_csv(session_id)

        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        # Vérifier colonnes requises
        required_columns = [
            'hotel_name', 'hotel_address', 'cvent_url',
            'nom_salle', 'surface', 'capacite_theatre',
            'capacite_u', 'capacite_banquet', 'capacite_cocktail',
            'capacite_classe', 'capacite_amphi'
        ]

        for col in required_columns:
            assert col in df.columns, f"Colonne manquante: {col}"

        # Vérifier qu'il n'y a pas de colonnes totalement vides
        for col in df.columns:
            if col.startswith('capacite_') or col in ['surface', 'nom_salle']:
                # Ces colonnes peuvent être vides pour certaines lignes
                continue

            # Les colonnes d'hôtel ne doivent pas être vides
            if col in ['hotel_name', 'hotel_address', 'cvent_url']:
                non_empty_count = df[col].dropna().astype(str).str.strip().str.len() > 0
                assert non_empty_count.any(), f"Colonne {col} entièrement vide"

        print("✅ Format CSV cohérent")

    def test_performance_large_session(self, db_service):
        """Test performance sur session plus importante"""
        # Créer session avec plus d'hôtels
        session_id = db_service.create_new_session("test_perf.csv", 10)

        hotels_data = [
            {
                'name': f'Hotel Performance {i}',
                'address': f'Ville {i}, BE',
                'url': f'http://test{i}.com'
            }
            for i in range(1, 11)
        ]

        # Insérer tous les hôtels
        hotel_ids = db_service.prepare_hotels_batch(session_id, hotels_data)

        # Simuler extractions sur quelques hôtels
        for i, hotel_id in enumerate(hotel_ids[:5]):
            if hotel_id:
                cvent_result = {
                    'success': True,
                    'data': {
                        'interface_type': 'grid_direct',
                        'headers': ['Salles de réunion', 'Taille', 'Théâtre'],
                        'rows': [
                            [f'Salle {i}-A', f'{20+i*5} m²', f'{30+i*10}']
                        ]
                    }
                }

                db_service.process_hotel_extraction(
                    hotel_id=hotel_id,
                    cvent_result=cvent_result
                )

        # Test export
        import time
        start_time = time.time()

        csv_content = db_service.export_session_to_csv(session_id)

        export_time = time.time() - start_time

        # Vérifications
        assert csv_content is not None
        assert len(csv_content) > 0

        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))

        assert len(df) >= 5  # Au moins 5 salles
        assert len(df['hotel_name'].unique()) >= 5  # Au moins 5 hôtels

        print(f"✅ Performance export OK: {len(df)} lignes en {export_time:.2f}s")

        # Nettoyer
        db_service.finalize_session(session_id, success=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])