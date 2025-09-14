"""
Tests unitaires pour le client Supabase
Teste les fonctions CRUD et la gestion d'erreurs
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import os
from datetime import datetime

# Import du module à tester
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError


class TestSupabaseClient:
    """Tests pour le client Supabase"""

    @patch('modules.supabase_client.create_client')
    def test_init_success(self, mock_create_client):
        """Test initialisation réussie du client"""
        mock_client = Mock()
        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()
            assert client.client == mock_client
            mock_create_client.assert_called_once()

    def test_init_missing_env_vars(self):
        """Test échec si variables environnement manquantes"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(SupabaseError, match="Variables SUPABASE_URL et SUPABASE_KEY requises"):
                SupabaseClient()

    @patch('modules.supabase_client.create_client')
    def test_create_extraction_session(self, mock_create_client):
        """Test création de session"""
        mock_client = Mock()
        mock_table = Mock()
        mock_client.table.return_value = mock_table
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_execute = Mock()
        mock_insert.execute.return_value = mock_execute
        mock_execute.data = [{'id': 'test-session-id'}]
        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()
            session_id = client.create_extraction_session(
                "Test Session", 10, "test.csv"
            )

            assert session_id == 'test-session-id'
            mock_table.insert.assert_called_once()

    @patch('modules.supabase_client.create_client')
    def test_insert_hotel(self, mock_create_client):
        """Test insertion d'hôtel"""
        mock_client = Mock()
        mock_table = Mock()
        mock_client.table.return_value = mock_table
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_execute = Mock()
        mock_insert.execute.return_value = mock_execute
        mock_execute.data = [{'id': 'hotel-id'}]
        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()
            hotel_id = client.insert_hotel(
                "session-id", "Test Hotel", "123 Test St", "https://test.cvent.com"
            )

            assert hotel_id == 'hotel-id'
            mock_client.table.assert_called_with("hotels")

    @patch('modules.supabase_client.create_client')
    def test_clean_capacity_value(self, mock_create_client):
        """Test nettoyage des valeurs de capacité"""
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()

            # Tests de valeurs valides
            assert client._clean_capacity_value(50) == 50
            assert client._clean_capacity_value("100") == 100
            assert client._clean_capacity_value("  75  ") == 75

            # Tests de valeurs invalides
            assert client._clean_capacity_value("-") is None
            assert client._clean_capacity_value("") is None
            assert client._clean_capacity_value("nan") is None
            assert client._clean_capacity_value(None) is None

    @patch('modules.supabase_client.create_client')
    def test_insert_meeting_rooms(self, mock_create_client):
        """Test insertion de salles de réunion"""
        mock_client = Mock()
        mock_table = Mock()
        mock_client.table.return_value = mock_table
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_execute = Mock()
        mock_insert.execute.return_value = mock_execute
        mock_execute.data = [{'id': 'room1'}, {'id': 'room2'}]
        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()

            rooms_data = [
                {
                    'nom_salle': 'Salle A',
                    'surface': '50 m²',
                    'capacite_u': '20',
                    'capacite_theatre': '-'
                },
                {
                    'nom_salle': 'Salle B',
                    'capacite_banquet': '30'
                }
            ]

            count = client.insert_meeting_rooms("hotel-id", rooms_data)
            assert count == 2

    @patch('modules.supabase_client.create_client')
    def test_retry_decorator(self, mock_create_client):
        """Test du décorateur retry"""
        mock_client = Mock()
        mock_table = Mock()
        mock_client.table.return_value = mock_table

        # Simuler 2 échecs puis succès
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_execute = Mock()
        mock_insert.execute.side_effect = [Exception("Erreur 1"), Exception("Erreur 2"), mock_execute]
        mock_execute.data = [{'id': 'test-id'}]
        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()

            # Le retry devrait fonctionner après 2 échecs
            with patch('time.sleep'):  # Mock sleep pour accélérer les tests
                session_id = client.create_extraction_session("Test", 1)
                assert session_id == 'test-id'

    @patch('modules.supabase_client.create_client')
    def test_insert_hotel_with_rooms_transaction_success(self, mock_create_client):
        """Test transaction réussie"""
        mock_client = Mock()

        # Mock pour update_hotel_status
        mock_hotels_table = Mock()
        mock_rooms_table = Mock()

        def table_side_effect(name):
            if name == "hotels":
                return mock_hotels_table
            elif name == "meeting_rooms":
                return mock_rooms_table

        mock_client.table.side_effect = table_side_effect

        # Mock pour hotels table
        mock_hotels_update = Mock()
        mock_hotels_table.update.return_value = mock_hotels_update
        mock_hotels_eq = Mock()
        mock_hotels_update.eq.return_value = mock_hotels_eq
        mock_hotels_eq.execute.return_value = Mock()

        # Mock pour rooms table
        mock_rooms_insert = Mock()
        mock_rooms_table.insert.return_value = mock_rooms_insert
        mock_rooms_execute = Mock()
        mock_rooms_insert.execute.return_value = mock_rooms_execute
        mock_rooms_execute.data = [{'id': 'room1'}]

        mock_create_client.return_value = mock_client

        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_KEY': 'test-key'
        }):
            client = SupabaseClient()

            hotel_data = {
                'id': 'hotel-id',
                'interface_type': 'grid',
                'salles_count': 1
            }

            rooms_data = [{'nom_salle': 'Salle Test'}]

            result = client.insert_hotel_with_rooms_transaction(
                hotel_data, rooms_data
            )

            assert result is True


class TestSupabaseError:
    """Tests pour la classe SupabaseError"""

    def test_supabase_error_creation(self):
        """Test création d'une SupabaseError"""
        error = SupabaseError("Test error message")
        assert str(error) == "Test error message"
        assert isinstance(error, Exception)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])