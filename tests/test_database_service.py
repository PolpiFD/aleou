"""
Tests unitaires pour le service de base de données
Teste la logique métier et les transactions
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import du module à tester
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from modules.database_service import DatabaseService
from modules.supabase_client import SupabaseError


class TestDatabaseService:
    """Tests pour le service de base de données"""

    @patch('modules.database_service.SupabaseClient')
    def test_init_success(self, mock_supabase_client):
        """Test initialisation réussie"""
        mock_client = Mock()
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()
        assert service.client == mock_client

    @patch('modules.database_service.SupabaseClient')
    def test_init_error(self, mock_supabase_client):
        """Test échec initialisation"""
        mock_supabase_client.side_effect = SupabaseError("Erreur connexion")

        with pytest.raises(SupabaseError):
            DatabaseService()

    @patch('modules.database_service.SupabaseClient')
    def test_create_new_session(self, mock_supabase_client):
        """Test création de nouvelle session"""
        mock_client = Mock()
        mock_client.create_extraction_session.return_value = "session-123"
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()
        session_id = service.create_new_session("test.csv", 10)

        assert session_id == "session-123"
        mock_client.create_extraction_session.assert_called_once()

    @patch('modules.database_service.SupabaseClient')
    def test_prepare_hotels_batch(self, mock_supabase_client):
        """Test préparation d'un batch d'hôtels"""
        mock_client = Mock()
        mock_client.insert_hotel.side_effect = ["hotel-1", "hotel-2", "hotel-3"]
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()

        hotels_data = [
            {'name': 'Hotel A', 'address': '123 St A', 'url': 'url-a'},
            {'name': 'Hotel B', 'address': '123 St B', 'url': 'url-b'},
            {'name': 'Hotel C', 'address': '123 St C', 'url': 'url-c'}
        ]

        hotel_ids = service.prepare_hotels_batch("session-123", hotels_data)

        assert hotel_ids == ["hotel-1", "hotel-2", "hotel-3"]
        assert mock_client.insert_hotel.call_count == 3

    @patch('modules.database_service.SupabaseClient')
    def test_map_cvent_data_to_db(self, mock_supabase_client):
        """Test mapping des données Cvent vers DB"""
        mock_client = Mock()
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()

        # Données simulées de Cvent
        headers = ['Salles de réunion', 'Taille', 'En U', 'Théâtre', 'En banquet']
        rows_data = [
            ['Salle Apollo', '50 m²', '20', '50', '40'],
            ['Salle Jupiter', '30 m²', '15', '30', '25']
        ]

        mapped_rooms = service.map_cvent_data_to_db(headers, rows_data)

        assert len(mapped_rooms) == 2

        # Vérifier le mapping du premier élément
        room1 = mapped_rooms[0]
        assert room1['nom_salle'] == 'Salle Apollo'
        assert room1['surface'] == '50 m²'
        assert room1['capacite_u'] == '20'
        assert room1['capacite_theatre'] == '50'
        assert room1['capacite_banquet'] == '40'

        # Vérifier le mapping du second élément
        room2 = mapped_rooms[1]
        assert room2['nom_salle'] == 'Salle Jupiter'
        assert room2['surface'] == '30 m²'

    @patch('modules.database_service.SupabaseClient')
    def test_map_cvent_data_column_variations(self, mock_supabase_client):
        """Test mapping avec différentes variations de colonnes"""
        mock_client = Mock()
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()

        # Test avec variations de noms de colonnes
        headers = ['Nom', 'Taille de la salle', 'en u', 'théâtre', 'Amphithéâtre']
        rows_data = [
            ['Salle Test', '25 m²', '10', '25', '15']
        ]

        mapped_rooms = service.map_cvent_data_to_db(headers, rows_data)

        assert len(mapped_rooms) == 1
        room = mapped_rooms[0]
        assert room['nom_salle'] == 'Salle Test'
        assert room['surface'] == '25 m²'
        assert room['capacite_u'] == '10'
        assert room['capacite_theatre'] == '25'
        assert room['capacite_amphi'] == '15'

    @patch('modules.database_service.SupabaseClient')
    def test_process_hotel_extraction_success(self, mock_supabase_client):
        """Test traitement réussi d'extraction d'hôtel"""
        mock_client = Mock()
        mock_client.insert_hotel_with_rooms_transaction.return_value = True
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()

        # Résultats simulés
        cvent_result = {
            'success': True,
            'data': {
                'interface_type': 'grid',
                'headers': ['Salles de réunion', 'Taille', 'En U'],
                'rows': [['Salle A', '30 m²', '15']]
            }
        }

        gmaps_result = {
            'extraction_status': 'success',
            'name': 'Hotel Test',
            'website': 'https://hotel-test.com'
        }

        result = service.process_hotel_extraction(
            "hotel-123",
            cvent_result=cvent_result,
            gmaps_result=gmaps_result
        )

        assert result is True
        mock_client.insert_hotel_with_rooms_transaction.assert_called_once()

    @patch('modules.database_service.SupabaseClient')
    def test_process_hotel_extraction_failure(self, mock_supabase_client):
        """Test échec traitement extraction d'hôtel"""
        mock_client = Mock()
        mock_client.insert_hotel_with_rooms_transaction.side_effect = Exception("DB Error")
        mock_client.update_hotel_status.return_value = None
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()

        cvent_result = {
            'success': True,
            'data': {
                'interface_type': 'grid',
                'headers': ['Salles de réunion'],
                'rows': [['Salle A']]
            }
        }

        result = service.process_hotel_extraction(
            "hotel-123",
            cvent_result=cvent_result
        )

        assert result is False
        # Vérifier que l'hôtel est marqué comme échoué
        mock_client.update_hotel_status.assert_called()

    @patch('modules.database_service.SupabaseClient')
    def test_get_session_statistics(self, mock_supabase_client):
        """Test récupération des statistiques de session"""
        mock_client = Mock()
        mock_stats = {
            'session_id': 'session-123',
            'total_hotels': 10,
            'completed': 7,
            'failed': 2,
            'pending': 1
        }
        mock_client.get_session_progress.return_value = mock_stats
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()
        stats = service.get_session_statistics("session-123")

        assert stats == mock_stats
        mock_client.get_session_progress.assert_called_once_with("session-123")

    @patch('modules.database_service.SupabaseClient')
    def test_finalize_session(self, mock_supabase_client):
        """Test finalisation de session"""
        mock_client = Mock()
        mock_client.get_session_progress.return_value = {'completed': 8, 'failed': 2}
        mock_client.update_session_status.return_value = None
        # Mock des accès tables Supabase
        hotels_table = MagicMock()
        hotels_table.select.return_value = hotels_table
        hotels_table.eq.return_value = hotels_table
        hotels_table.execute.return_value = MagicMock(data=[
            {'id': 'hotel-1', 'extraction_status': 'completed'},
            {'id': 'hotel-2', 'extraction_status': 'completed'},
            {'id': 'hotel-3', 'extraction_status': 'completed'},
            {'id': 'hotel-4', 'extraction_status': 'completed'},
            {'id': 'hotel-5', 'extraction_status': 'completed'},
            {'id': 'hotel-6', 'extraction_status': 'completed'},
            {'id': 'hotel-7', 'extraction_status': 'completed'},
            {'id': 'hotel-8', 'extraction_status': 'completed'},
            {'id': 'hotel-9', 'extraction_status': 'failed'},
            {'id': 'hotel-10', 'extraction_status': 'failed'},
        ])

        sessions_table = MagicMock()
        sessions_table.select.return_value = sessions_table
        sessions_table.eq.return_value = sessions_table
        sessions_table.execute.return_value = MagicMock(data=[
            {'id': 'session-123', 'total_hotels': 10}
        ])

        client_wrapper = MagicMock()

        def table_side_effect(table_name):
            if table_name == "hotels":
                return hotels_table
            if table_name == "extraction_sessions":
                return sessions_table
            return MagicMock()

        client_wrapper.table.side_effect = table_side_effect
        mock_client.client = client_wrapper
        mock_supabase_client.return_value = mock_client

        service = DatabaseService()
        service.finalize_session("session-123", success=True)

        mock_client.update_session_status.assert_called_once_with(
            session_id="session-123",
            status="completed",
            processed_hotels=10
        )

    def test_column_mapping_constants(self):
        """Test des constantes de mapping des colonnes"""
        # Vérifier que les mappings critiques existent
        assert DatabaseService.COLUMN_MAPPING['Taille'] == 'surface'
        assert DatabaseService.COLUMN_MAPPING['En U'] == 'capacite_u'
        assert DatabaseService.COLUMN_MAPPING['Théâtre'] == 'capacite_theatre'
        assert DatabaseService.COLUMN_MAPPING['En banquet'] == 'capacite_banquet'
        assert DatabaseService.COLUMN_MAPPING['En cocktail'] == 'capacite_cocktail'
        assert DatabaseService.COLUMN_MAPPING['Salle de classe'] == 'capacite_classe'
        assert DatabaseService.COLUMN_MAPPING['Amphithéâtre'] == 'capacite_amphi'

        # Vérifier que les champs ignorés sont bien None
        assert DatabaseService.COLUMN_MAPPING['Capacité maximum'] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])