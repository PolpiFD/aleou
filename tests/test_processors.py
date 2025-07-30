"""
Tests unitaires pour les modules processors refactorisés
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from modules.processors import HotelProcessor, DataExtractor, ResultsManager


class TestHotelProcessor:
    """Tests pour HotelProcessor"""
    
    @pytest.fixture
    def processor(self):
        return HotelProcessor()
    
    @pytest.fixture
    def sample_hotel_data(self):
        return {
            'name': 'Test Hotel Brussels',
            'address': '123 Rue de Test, Brussels, Belgium'
        }
    
    @pytest.mark.asyncio
    async def test_process_hotel_all_enabled(self, processor, sample_hotel_data):
        """Test traitement complet d'un hôtel"""
        
        # Mock des extracteurs
        with patch('modules.processors.hotel_processor.extract_cvent_data') as mock_cvent, \
             patch('modules.processors.hotel_processor.extract_hotels_batch') as mock_gmaps, \
             patch('modules.processors.hotel_processor.extract_hotels_websites_batch') as mock_website:
            
            # Configuration des mocks
            mock_cvent.return_value = {'success': True, 'meeting_rooms': [{'name': 'Room A'}]}
            mock_gmaps.return_value = [{'success': True, 'website': 'https://test-hotel.com'}]
            mock_website.return_value = [{'success': True, 'description': 'Great hotel'}]
            
            # Test
            result = await processor.process_hotel(
                sample_hotel_data,
                enable_cvent=True,
                enable_gmaps=True, 
                enable_website=True
            )
            
            # Vérifications
            assert result['success'] == True
            assert result['hotel_data'] == sample_hotel_data
            assert result['cvent_data']['success'] == True
            assert result['gmaps_data']['success'] == True  
            assert result['website_data']['success'] == True
            assert result['processing_time'] > 0
            assert 'timestamp' in result
    
    @pytest.mark.asyncio
    async def test_process_hotel_partial_success(self, processor, sample_hotel_data):
        """Test avec succès partiel"""
        
        with patch('modules.processors.hotel_processor.extract_cvent_data') as mock_cvent, \
             patch('modules.processors.hotel_processor.extract_hotels_batch') as mock_gmaps, \
             patch('modules.processors.hotel_processor.extract_hotels_websites_batch') as mock_website:
            
            # Cvent échoue, autres réussissent
            mock_cvent.side_effect = Exception("Cvent timeout")
            mock_gmaps.return_value = [{'success': True, 'website': 'https://test-hotel.com'}]
            mock_website.return_value = [{'success': True, 'description': 'Great hotel'}]
            
            result = await processor.process_hotel(sample_hotel_data)
            
            # Toujours succès si au moins 1 extraction réussit
            assert result['success'] == True
            assert result['cvent_data'] is None
            assert result['gmaps_data']['success'] == True
            assert result['website_data']['success'] == True
    
    @pytest.mark.asyncio
    async def test_process_hotel_all_failed(self, processor, sample_hotel_data):
        """Test avec tous les extracteurs échouant"""
        
        with patch('modules.processors.hotel_processor.extract_cvent_data') as mock_cvent, \
             patch('modules.processors.hotel_processor.extract_hotels_batch') as mock_gmaps, \
             patch('modules.processors.hotel_processor.extract_hotels_websites_batch') as mock_website:
            
            # Tous échouent
            mock_cvent.side_effect = Exception("Cvent error")
            mock_gmaps.return_value = [{'success': False, 'error': 'GMaps error'}]
            mock_website.return_value = [{'success': False, 'error': 'Website error'}]
            
            result = await processor.process_hotel(sample_hotel_data)
            
            # Échec global
            assert result['success'] == False
            assert result['cvent_data'] is None
            assert result['gmaps_data']['success'] == False
            assert result['website_data']['success'] == False
    
    def test_calculate_success_logic(self, processor):
        """Test logique de calcul du succès"""
        
        # Cas 1: Tout réussi
        result_all_success = {
            'cvent_data': {'success': True},
            'gmaps_data': {'success': True},
            'website_data': {'success': True}
        }
        assert processor._calculate_success(result_all_success, True, True, True) == True
        
        # Cas 2: Partiel (1 sur 3)
        result_partial = {
            'cvent_data': None,
            'gmaps_data': {'success': True},
            'website_data': {'success': False}
        }
        assert processor._calculate_success(result_partial, True, True, True) == True
        
        # Cas 3: Aucun succès
        result_none = {
            'cvent_data': None,
            'gmaps_data': {'success': False},
            'website_data': {'success': False}
        }
        assert processor._calculate_success(result_none, True, True, True) == False
    
    def test_get_stats(self, processor):
        """Test récupération des statistiques"""
        stats = processor.get_stats()
        
        expected_keys = ['cvent_time', 'gmaps_time', 'website_time', 'total_time', 'errors']
        for key in expected_keys:
            assert key in stats


class TestDataExtractor:
    """Tests pour DataExtractor"""
    
    @pytest.fixture
    def extractor(self):
        return DataExtractor()
    
    @pytest.fixture
    def sample_hotels_data(self):
        return [
            {'name': 'Hotel A', 'address': 'Address A'},
            {'name': 'Hotel B', 'address': 'Address B'},
            {'name': 'Hotel C', 'address': 'Address C'}
        ]
    
    @pytest.mark.asyncio
    async def test_extract_hotels_parallel(self, extractor, sample_hotels_data):
        """Test extraction parallèle"""
        
        # Mock HotelProcessor
        with patch('modules.processors.data_extractor.HotelProcessor') as MockProcessor:
            mock_processor_instance = AsyncMock()
            mock_processor_instance.process_hotel.return_value = {
                'success': True,
                'hotel_data': sample_hotels_data[0],
                'processing_time': 1.0,
                'cvent_data': {'success': True}
            }
            MockProcessor.return_value = mock_processor_instance
            
            results = await extractor.extract_hotels_parallel(
                sample_hotels_data,
                enable_cvent=True,
                enable_gmaps=False,
                enable_website=False
            )
            
            # Vérifications
            assert len(results) == 3
            assert all(r['success'] for r in results)
            
            # Vérifier que process_hotel a été appelé pour chaque hôtel
            assert mock_processor_instance.process_hotel.call_count == 3
    
    def test_create_batches(self, extractor):
        """Test création de batches"""
        items = list(range(25))
        batches = extractor._create_batches(items, batch_size=10)
        
        assert len(batches) == 3  # 25 items en batches de 10
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10  
        assert len(batches[2]) == 5  # Dernier batch partiel
        
        # Vérifier continuité
        all_items = []
        for batch in batches:
            all_items.extend(batch)
        assert all_items == items
    
    def test_calculate_final_stats(self, extractor):
        """Test calcul des statistiques finales"""
        
        # Données de test
        mock_results = [
            {
                'success': True, 
                'processing_time': 2.0,
                'cvent_data': {'success': True},
                'gmaps_data': {'success': False},  
                'website_data': None
            },
            {
                'success': False,
                'processing_time': 1.5,
                'cvent_data': {'success': False},
                'gmaps_data': {'success': True},
                'website_data': {'success': True}
            }
        ]
        
        extractor._calculate_final_stats(mock_results, total_time=5.0)
        
        # Vérifications
        assert extractor.stats['total_hotels'] == 2
        assert extractor.stats['successful_hotels'] == 1
        assert extractor.stats['failed_hotels'] == 1
        assert extractor.stats['total_time'] == 5.0
        assert extractor.stats['avg_time_per_hotel'] == 2.5
        
        # Stats par type d'extraction
        assert extractor.stats['extraction_stats']['cvent']['success'] == 1
        assert extractor.stats['extraction_stats']['cvent']['failed'] == 1
        assert extractor.stats['extraction_stats']['gmaps']['success'] == 1
        assert extractor.stats['extraction_stats']['website']['success'] == 1
    
    def test_get_performance_summary(self, extractor):
        """Test génération du résumé de performances"""
        
        # Simuler des stats
        extractor.stats = {
            'total_hotels': 10,
            'successful_hotels': 8,
            'total_time': 50.0,
            'avg_time_per_hotel': 5.0,
            'extraction_stats': {
                'cvent': {'success': 6, 'failed': 4, 'avg_time': 3.0},
                'gmaps': {'success': 9, 'failed': 1, 'avg_time': 1.0},
                'website': {'success': 7, 'failed': 3, 'avg_time': 4.0}
            }
        }
        
        summary = extractor.get_performance_summary()
        
        # Vérifications
        assert summary['total_hotels'] == 10
        assert summary['successful_hotels'] == 8
        assert summary['success_rate'] == "80.0%"
        assert summary['throughput'] == "0.2 hôtels/s"
        
        # Vérifier breakdown par extraction
        assert 'cvent' in summary['extraction_breakdown']
        assert 'gmaps' in summary['extraction_breakdown']
        assert 'website' in summary['extraction_breakdown']


class TestResultsManager:
    """Tests pour ResultsManager"""
    
    @pytest.fixture  
    def manager(self):
        return ResultsManager(output_dir="test_output")
    
    @pytest.fixture
    def sample_extraction_results(self):
        return [
            {
                'hotel_data': {'name': 'Hotel A', 'address': 'Address A'},
                'success': True,
                'processing_time': 2.0,
                'timestamp': '2024-01-01T12:00:00',
                'errors': [],
                'cvent_data': {
                    'success': True,
                    'meeting_rooms': [{'name': 'Room 1', 'capacity': 50}],
                    'venue_id': 'venue123'
                },
                'gmaps_data': {
                    'success': True,
                    'phone': '+32 2 123 4567',
                    'website': 'https://hotel-a.com',
                    'rating': '4.5',
                    'place_id': 'place123'
                },
                'website_data': {
                    'success': True,
                    'description': 'Great hotel in Brussels',
                    'facilities': 'WiFi, Parking, Restaurant',
                    'email': 'info@hotel-a.com'
                }
            },
            {
                'hotel_data': {'name': 'Hotel B', 'address': 'Address B'},
                'success': False,
                'processing_time': 1.0,
                'timestamp': '2024-01-01T12:01:00',
                'errors': ['Network timeout'],
                'cvent_data': {'success': False, 'error': 'Timeout'},
                'gmaps_data': {'success': False, 'error': 'Not found'},
                'website_data': {'success': False, 'error': 'Access denied'}
            }
        ]
    
    def test_consolidate_results(self, manager, sample_extraction_results):
        """Test consolidation des résultats"""
        
        consolidated = manager.consolidate_results(
            sample_extraction_results,
            include_cvent=True,
            include_gmaps=True,
            include_website=True
        )
        
        # Vérifications générales
        assert len(consolidated) == 2
        
        # Premier hôtel (succès)
        hotel_a = consolidated[0]
        assert hotel_a['name'] == 'Hotel A'
        assert hotel_a['extraction_success'] == True
        assert hotel_a['phone'] == '+32 2 123 4567'
        assert hotel_a['website'] == 'https://hotel-a.com'
        assert hotel_a['email'] == 'info@hotel-a.com'
        assert hotel_a['meeting_rooms_count'] == 1
        assert 'Room 1' in hotel_a['meeting_rooms_details']
        assert hotel_a['cvent_extraction_success'] == True
        assert hotel_a['gmaps_extraction_success'] == True
        assert hotel_a['website_extraction_success'] == True
        
        # Deuxième hôtel (échec)
        hotel_b = consolidated[1]
        assert hotel_b['name'] == 'Hotel B'
        assert hotel_b['extraction_success'] == False
        assert 'Network timeout' in hotel_b['errors']
        assert hotel_b['cvent_extraction_success'] == False
        assert hotel_b['gmaps_extraction_success'] == False
        assert hotel_b['website_extraction_success'] == False
        
        # Métadonnées
        assert manager.metadata['total_hotels'] == 2
        assert manager.metadata['successful_extractions'] == 1
        assert set(manager.metadata['data_sources']) == {'cvent', 'gmaps', 'website'}
    
    def test_format_meeting_rooms(self, manager):
        """Test formatage des salles de réunion"""
        
        meeting_rooms = [
            {'name': 'Room A', 'capacity': 50, 'size': '100m²'},
            {'name': 'Room B', 'capacity': 25},
            {'name': 'Room C', 'size': '50m²'}
        ]
        
        formatted = manager._format_meeting_rooms(meeting_rooms)
        
        expected = "Room A (50 pers.) - 100m² | Room B (25 pers.) | Room C - 50m²"
        assert formatted == expected
        
        # Cas vide
        assert manager._format_meeting_rooms([]) == ""
        assert manager._format_meeting_rooms(None) == ""
    
    def test_clean_csv_row(self, manager):
        """Test nettoyage des données pour CSV"""
        
        dirty_row = {
            'name': 'Hotel\nWith\tTabs',
            'description': 'A' * 1500,  # Trop long
            'data': {'complex': 'object'},
            'list': [1, 2, 3],
            'none_value': None,
            'number': 42
        }
        
        cleaned = manager._clean_csv_row(dirty_row)
        
        # Vérifications
        assert cleaned['name'] == 'Hotel With Tabs'  # Tabs/newlines supprimés
        assert len(cleaned['description']) == 1000  # Tronqué avec ...
        assert cleaned['description'].endswith('...')
        assert cleaned['data'] == "{'complex': 'object'}"  # Dict converti en string
        assert cleaned['list'] == "[1, 2, 3]"  # List convertie en string
        assert cleaned['none_value'] == ''  # None devient vide
        assert cleaned['number'] == '42'  # Number converti en string
    
    def test_get_consolidation_stats(self, manager, sample_extraction_results):
        """Test statistiques de consolidation"""
        
        manager.consolidate_results(sample_extraction_results)
        stats = manager.get_consolidation_stats()
        
        assert stats['total_hotels'] == 2
        assert stats['successful_extractions'] == 1
        assert stats['success_rate'] == "50.0%"
        assert 'data_completeness' in stats
        assert 'phone' in stats['data_completeness']
        assert 'email' in stats['data_completeness']
        assert 'website' in stats['data_completeness']
        assert 'meeting_rooms' in stats['data_completeness']
    
    @pytest.mark.asyncio
    async def test_export_csv_streaming_vs_normal(self, manager, sample_extraction_results):
        """Test comparaison export normal vs streaming"""
        
        manager.consolidate_results(sample_extraction_results)
        
        # Export normal
        csv_path_normal = manager.export_to_csv(filename="test_normal.csv", streaming=False)
        
        # Export streaming
        csv_path_streaming = manager.export_to_csv(filename="test_streaming.csv", streaming=True)
        
        # Vérifier que les deux fichiers existent
        assert manager.output_dir.joinpath("test_normal.csv").exists()
        assert manager.output_dir.joinpath("test_streaming.csv").exists()
        
        # Nettoyer après test
        manager.output_dir.joinpath("test_normal.csv").unlink()
        manager.output_dir.joinpath("test_streaming.csv").unlink()
        if manager.output_dir.exists():
            manager.output_dir.rmdir()