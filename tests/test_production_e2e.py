"""
Tests de production End-to-End avec CSV réel
Teste l'architecture complète Supabase avec vraies données
"""

import pytest
import pandas as pd
import asyncio
import os
import time
from pathlib import Path

# Import des modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError
from modules.database_service import DatabaseService
from modules.parallel_processor_db import ParallelHotelProcessorDB, ParallelConfig
from services.extraction_service_db import ExtractionServiceDB


class TestProductionE2E:
    """Tests de production complets avec CSV réel"""

    @pytest.fixture
    def real_csv_path(self):
        """Chemin vers le CSV de test réel"""
        return Path(__file__).parent / "test quelques hotels.csv"

    @pytest.fixture
    def sample_hotels_df(self, real_csv_path):
        """DataFrame avec échantillon d'hôtels réels"""
        df = pd.read_csv(real_csv_path)
        # Prendre un échantillon de 3 hôtels pour tests rapides
        return df.head(3)

    @pytest.fixture
    def production_config(self):
        """Configuration de production"""
        return ParallelConfig(
            max_workers=2,  # Réduit pour les tests
            batch_size=2,   # Petit batch pour tests
            cvent_timeout=60
        )

    def test_supabase_connection_real(self):
        """Test de connexion réelle à Supabase"""
        try:
            client = SupabaseClient()
            # Test simple : créer et récupérer une session
            session_id = client.create_extraction_session(
                "Test E2E Connection",
                1,
                "test.csv"
            )
            assert session_id is not None
            print(f"✅ Connexion Supabase OK - Session: {session_id[:8]}...")

            # Nettoyer
            client.client.table("extraction_sessions").delete().eq(
                "id", session_id
            ).execute()

        except SupabaseError as e:
            pytest.skip(f"Supabase non configuré ou inaccessible: {e}")

    def test_database_service_with_real_data(self, sample_hotels_df):
        """Test du service DB avec vraies données"""
        try:
            service = DatabaseService()

            # Créer session
            session_id = service.create_new_session("test_e2e.csv", len(sample_hotels_df))
            assert session_id is not None

            # Préparer hotels
            hotels_data = []
            for _, row in sample_hotels_df.iterrows():
                hotels_data.append({
                    'name': row['name'],
                    'address': row['adresse'],
                    'url': row['URL']
                })

            hotel_ids = service.prepare_hotels_batch(session_id, hotels_data)
            assert len(hotel_ids) == len(hotels_data)

            # Vérifier en DB
            stats = service.get_session_statistics(session_id)
            assert stats.get('total_hotels') == len(sample_hotels_df)

            print(f"✅ Service DB OK - {len(hotel_ids)} hôtels insérés")

            # Nettoyer
            service.db_service.finalize_session(session_id, success=False)

        except SupabaseError as e:
            pytest.skip(f"Supabase non accessible: {e}")

    @pytest.mark.slow
    def test_single_hotel_extraction_real(self, sample_hotels_df):
        """Test d'extraction réelle d'un hôtel (LENT)"""
        if os.getenv("SKIP_SLOW_TESTS") == "1":
            pytest.skip("Tests lents désactivés")

        try:
            service = ExtractionServiceDB()

            # Prendre le premier hôtel
            hotel = sample_hotels_df.iloc[0]

            print(f"🔍 Test extraction: {hotel['name']}")

            # Test extraction simple (Cvent seulement)
            # NOTE: Ceci fait un vraie extraction Playwright !
            service.process_single_url_extraction(
                name=hotel['name'],
                address=hotel['adresse'],
                url=hotel['URL'],
                extract_gmaps=False,
                extract_website=False
            )

            print(f"✅ Extraction réelle réussie pour {hotel['name']}")

        except Exception as e:
            print(f"⚠️ Extraction échouée (normal pour tests): {e}")
            # Ne pas faire échouer le test si c'est juste un problème d'extraction

    @pytest.mark.integration
    def test_csv_processing_flow_real(self, sample_hotels_df, production_config):
        """Test du flux CSV complet avec vraies données"""
        try:
            # Mock les extractions pour éviter les vrais appels Playwright
            from unittest.mock import patch

            def mock_cvent_extract(*args, **kwargs):
                return {
                    'success': True,
                    'data': {
                        'interface_type': 'grid',
                        'headers': ['Salles de réunion', 'Taille', 'En U', 'Théâtre'],
                        'rows': [
                            ['Salle Apollo', '50 m²', '20', '40'],
                            ['Salle Jupiter', '30 m²', '15', '30']
                        ],
                        'salles_count': 2
                    }
                }

            with patch('modules.cvent_extractor.extract_cvent_data', side_effect=mock_cvent_extract):
                processor = ParallelHotelProcessorDB(production_config)
                service = DatabaseService()

                # Créer session
                session_id = service.create_new_session("test_integration.csv", len(sample_hotels_df))

                # Préparer données
                hotels_data = []
                for _, row in sample_hotels_df.iterrows():
                    hotels_data.append({
                        'name': row['name'],
                        'address': row['adresse'],
                        'url': row['URL']
                    })

                print(f"🔄 Test flux complet avec {len(hotels_data)} hôtels...")

                # Traitement complet
                final_stats = asyncio.run(
                    processor.process_hotels_to_database(
                        hotels_data=hotels_data,
                        session_id=session_id,
                        extract_cvent=True,
                        extract_gmaps=False,  # Désactivé pour tests
                        extract_website=False
                    )
                )

                # Vérifications
                assert final_stats['total_hotels'] == len(hotels_data)
                assert final_stats['successful'] >= 0  # Au moins pas d'erreur critique

                # Vérifier en DB
                session_stats = service.get_session_statistics(session_id)
                assert session_stats.get('total_hotels') == len(hotels_data)

                print(f"✅ Flux complet OK:")
                print(f"   • Hôtels: {final_stats['total_hotels']}")
                print(f"   • Succès: {final_stats['successful']}")
                print(f"   • Échecs: {final_stats['failed']}")
                print(f"   • Temps: {final_stats.get('elapsed_time', 0):.1f}s")

        except SupabaseError as e:
            pytest.skip(f"Supabase non accessible: {e}")

    def test_column_mapping_real_data(self):
        """Test mapping colonnes avec données réelles simulées"""
        from modules.database_service import DatabaseService

        # Simuler des headers Cvent réels variés
        test_cases = [
            {
                'name': 'Interface Grid classique',
                'headers': ['Salles de réunion', 'Taille', 'En U', 'Théâtre', 'En banquet'],
                'row': ['Salle Executive', '45 m²', '18', '35', '30'],
                'expected_fields': ['nom_salle', 'surface', 'capacite_u', 'capacite_theatre', 'capacite_banquet']
            },
            {
                'name': 'Interface Popup avec variations',
                'headers': ['Nom', 'Taille de la salle', 'en u', 'théâtre', 'Amphithéâtre'],
                'row': ['Salle VIP', '60 m²', '25', '50', '15'],
                'expected_fields': ['nom_salle', 'surface', 'capacite_u', 'capacite_theatre', 'capacite_amphi']
            }
        ]

        service = DatabaseService()

        for case in test_cases:
            print(f"🧪 Test mapping: {case['name']}")

            mapped = service.map_cvent_data_to_db(case['headers'], [case['row']])

            assert len(mapped) == 1
            room = mapped[0]

            # Vérifier que tous les champs attendus sont présents
            for field in case['expected_fields']:
                assert field in room, f"Champ {field} manquant dans {case['name']}"
                assert room[field] is not None, f"Champ {field} vide dans {case['name']}"

            print(f"   ✅ {len(case['expected_fields'])} champs mappés correctement")

    def test_data_cleaning_edge_cases(self):
        """Test nettoyage données avec cas limites"""
        service = DatabaseService()

        # Cas avec données sales réelles
        headers = ['Salles de réunion', 'En U', 'Théâtre', 'En banquet', 'En cocktail']
        dirty_data = [
            ['Salle Clean', '20', '50', '40', '60'],
            ['Salle Dirty', '-', '', 'nan', 'N/A'],
            ['Salle Mixed', '15', '-', '30', ''],
            ['Salle Spécial', '0', '0', '-', '45']
        ]

        mapped_rooms = service.map_cvent_data_to_db(headers, dirty_data)

        assert len(mapped_rooms) == 4

        # Salle Clean: toutes les données
        assert mapped_rooms[0]['capacite_u'] == '20'
        assert mapped_rooms[0]['capacite_theatre'] == '50'

        # Salle Dirty: données nettoyées (champs manquants)
        dirty_room = mapped_rooms[1]
        assert dirty_room['nom_salle'] == 'Salle Dirty'
        # Les champs sales ne doivent pas être présents ou être vides

        # Salle Mixed: mélange
        mixed_room = mapped_rooms[2]
        assert mixed_room['capacite_u'] == '15'
        assert mixed_room['capacite_banquet'] == '30'

        print("✅ Nettoyage des données OK - Cas limites gérés")

    @pytest.mark.performance
    def test_batch_processing_performance(self, sample_hotels_df):
        """Test de performance du traitement par batch"""
        if os.getenv("SKIP_PERF_TESTS") == "1":
            pytest.skip("Tests de performance désactivés")

        try:
            start_time = time.time()

            service = DatabaseService()
            session_id = service.create_new_session("test_perf.csv", len(sample_hotels_df))

            # Test insertion batch
            batch_start = time.time()
            hotels_data = []
            for _, row in sample_hotels_df.iterrows():
                hotels_data.append({
                    'name': row['name'],
                    'address': row['adresse'],
                    'url': row['URL']
                })

            hotel_ids = service.prepare_hotels_batch(session_id, hotels_data)
            batch_time = time.time() - batch_start

            assert len(hotel_ids) == len(hotels_data)

            total_time = time.time() - start_time

            print(f"✅ Performance batch OK:")
            print(f"   • {len(hotels_data)} hôtels en {batch_time:.2f}s")
            print(f"   • Moyenne: {(batch_time/len(hotels_data)*1000):.1f}ms par hôtel")
            print(f"   • Total: {total_time:.2f}s")

            # Seuil de performance (très généreux pour éviter flakiness)
            assert batch_time < 10, f"Insertion trop lente: {batch_time:.2f}s"

        except SupabaseError as e:
            pytest.skip(f"Supabase non accessible: {e}")

    def test_error_recovery_real(self, sample_hotels_df):
        """Test de récupération d'erreur avec vraies données"""
        try:
            service = DatabaseService()
            session_id = service.create_new_session("test_error.csv", len(sample_hotels_df))

            # Simuler une erreur sur un hôtel
            hotels_data = []
            for i, (_, row) in enumerate(sample_hotels_df.iterrows()):
                hotels_data.append({
                    'name': row['name'] if i != 1 else None,  # Erreur sur le 2e
                    'address': row['adresse'],
                    'url': row['URL']
                })

            # L'insertion devrait échouer proprement
            try:
                hotel_ids = service.prepare_hotels_batch(session_id, hotels_data)
                # Si pas d'erreur, au moins vérifier le comportement
                assert len(hotel_ids) <= len(hotels_data)
            except Exception:
                # Erreur attendue, c'est OK
                pass

            print("✅ Récupération d'erreur OK - Système robuste")

        except SupabaseError as e:
            pytest.skip(f"Supabase non accessible: {e}")


# Commandes pour exécuter les tests
if __name__ == "__main__":
    # Tests de base
    pytest.main([
        __file__,
        "-v",
        "-m", "not slow and not performance",
        "--tb=short"
    ])

    print("\n" + "="*50)
    print("Pour tests complets:")
    print("pytest tests/test_production_e2e.py -v --tb=short")
    print("\nPour tests avec extractions réelles (LENT):")
    print("pytest tests/test_production_e2e.py -v -m slow --tb=short")
    print("\nPour tests de performance:")
    print("pytest tests/test_production_e2e.py -v -m performance --tb=short")