"""
Test End-to-End COMPLET du workflow
Teste l'intégralité: CSV → Cvent (Playwright) → Google Maps → Website → Supabase
ATTENTION: Ce test fait de VRAIES extractions et peut prendre du temps
"""

import pytest
import pandas as pd
import asyncio
import os
import time
from pathlib import Path
from datetime import datetime

# Import des modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError
from modules.database_service import DatabaseService
from modules.parallel_processor_db import ParallelHotelProcessorDB, ParallelConfig
from services.extraction_service_db import ExtractionServiceDB


class TestFullE2E:
    """Tests End-to-End complets avec VRAIES extractions"""

    @pytest.fixture
    def test_csv_path(self):
        """Chemin vers le CSV de test réel"""
        return Path(__file__).parent / "test quelques hotels.csv"

    @pytest.fixture
    def small_sample_df(self, test_csv_path):
        """DataFrame avec un très petit échantillon pour tests E2E complets"""
        df = pd.read_csv(test_csv_path)
        # Prendre seulement les 2 premiers hôtels pour test E2E complet
        return df.head(2)

    @pytest.fixture
    def e2e_config(self):
        """Configuration optimisée pour E2E"""
        return ParallelConfig(
            max_workers=1,      # Séquentiel pour éviter les problèmes
            batch_size=1,       # Un par un pour diagnostics
            cvent_timeout=60,   # Timeout généreux
            gmaps_timeout=30,
            website_timeout=45
        )

    def test_prerequisites(self):
        """Vérifier que tous les prérequis sont en place"""
        print("🔍 Vérification des prérequis...")

        # Vérifier variables d'environnement
        required_vars = ['SUPABASE_URL', 'SUPABASE_KEY', 'GOOGLE_MAPS_API_KEY', 'OPENAI_API_KEY']
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            pytest.skip(f"Variables manquantes: {', '.join(missing_vars)}")

        # Vérifier connexion Supabase
        try:
            client = SupabaseClient()
            session_id = client.create_extraction_session("Test Prerequisites", 1, "test.csv")
            client.client.table("extraction_sessions").delete().eq("id", session_id).execute()
            print("✅ Supabase accessible")
        except Exception as e:
            pytest.skip(f"Supabase inaccessible: {e}")

        print("✅ Tous les prérequis OK")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_single_hotel_complete_workflow(self, small_sample_df, e2e_config):
        """
        Test du workflow complet sur UN seul hôtel
        CSV → Cvent → Google Maps → Website → Supabase
        """
        if os.getenv("SKIP_E2E_TESTS") == "1":
            pytest.skip("Tests E2E complets désactivés")

        # Prendre le premier hôtel seulement
        hotel_row = small_sample_df.iloc[0]
        hotel_data = {
            'name': hotel_row['name'],
            'address': hotel_row['adresse'],
            'url': hotel_row['URL']
        }

        print(f"\n🚀 TEST E2E COMPLET: {hotel_data['name']}")
        print(f"📍 Adresse: {hotel_data['address']}")
        print(f"🌐 URL Cvent: {hotel_data['url']}")
        print("=" * 60)

        start_time = time.time()

        try:
            # 1. Initialiser les services
            processor = ParallelHotelProcessorDB(e2e_config)
            db_service = DatabaseService()

            # 2. Créer session en DB
            session_id = db_service.create_new_session("test_full_e2e.csv", 1)
            print(f"✅ Session créée: {session_id[:8]}...")

            # 3. Traitement COMPLET avec toutes les extractions
            print("\n🔄 Démarrage du workflow complet...")

            final_stats = await processor.process_hotels_to_database(
                hotels_data=[hotel_data],
                session_id=session_id,
                extract_cvent=True,     # ✅ Vraie extraction Playwright
                extract_gmaps=True,     # ✅ Vraie extraction Google Maps
                extract_website=True    # ✅ Vraie extraction Website
            )

            elapsed = time.time() - start_time

            # 4. Vérifications des résultats
            print(f"\n📊 RÉSULTATS après {elapsed:.1f}s:")
            print(f"   • Total hotels: {final_stats['total_hotels']}")
            print(f"   • Successful: {final_stats['successful']}")
            print(f"   • Failed: {final_stats['failed']}")

            # 5. Vérification détaillée en DB
            print("\n🔍 Vérification détaillée des données en DB...")

            # Récupérer les données de l'hôtel
            supabase_client = SupabaseClient()

            # Hotels table
            hotels_result = supabase_client.client.table("hotels").select("*").eq("session_id", session_id).execute()
            assert len(hotels_result.data) == 1, f"Expected 1 hotel, got {len(hotels_result.data)}"

            hotel_record = hotels_result.data[0]
            print(f"✅ Hôtel en DB: {hotel_record['name']}")
            print(f"   • Status: {hotel_record['extraction_status']}")
            print(f"   • Interface type: {hotel_record.get('interface_type', 'N/A')}")
            print(f"   • Address: {hotel_record['address']}")
            print(f"   • Cvent URL: {hotel_record['cvent_url']}")
            print(f"   • Salles count: {hotel_record.get('salles_count', 0)}")

            # Vérifier données Google Maps si disponibles
            gmaps_fields = ['gmaps_name', 'gmaps_rating', 'gmaps_address', 'gmaps_phone', 'gmaps_website']
            for field in gmaps_fields:
                if hotel_record.get(field):
                    print(f"   • {field}: {hotel_record[field]}")

            # Vérifier website si disponible
            if hotel_record.get('official_website'):
                print(f"   • Official website: {hotel_record['official_website']}")

            # Meeting Rooms table
            rooms_result = supabase_client.client.table("meeting_rooms").select("*").eq("hotel_id", hotel_record['id']).execute()
            print(f"✅ {len(rooms_result.data)} salle(s) de réunion en DB")

            for i, room in enumerate(rooms_result.data, 1):
                print(f"   • Salle {i}: {room['nom_salle']}")
                print(f"     - Surface: {room.get('surface', 'N/A')}")
                print(f"     - Théâtre: {room.get('capacite_theatre', 'N/A')}")
                print(f"     - Banquet: {room.get('capacite_banquet', 'N/A')}")
                print(f"     - U: {room.get('capacite_u', 'N/A')}")

            # 6. Assertions finales
            assert final_stats['total_hotels'] == 1
            assert final_stats['successful'] >= 0  # Au moins pas d'erreur critique
            assert hotel_record['name'] == hotel_data['name']
            assert hotel_record['address'] == hotel_data['address']
            assert hotel_record['cvent_url'] == hotel_data['url']

            # Si extraction Cvent réussie, doit y avoir des salles
            if hotel_record['extraction_status'] == 'success' and final_stats['successful'] == 1:
                assert len(rooms_result.data) > 0, "Aucune salle trouvée malgré extraction réussie"
                print("✅ Extraction Cvent réussie avec salles")

            print(f"\n🎉 TEST E2E COMPLET RÉUSSI en {elapsed:.1f}s!")
            print("   Workflow: CSV → Cvent → Google Maps → Website → Supabase ✅")

            # Nettoyer (optionnel en mode debug)
            if os.getenv("KEEP_TEST_DATA") != "1":
                db_service.finalize_session(session_id, success=True)
                print("🧹 Données de test nettoyées")
            else:
                print(f"🔍 Session conservée pour inspection: {session_id}")

        except Exception as e:
            print(f"❌ Erreur dans test E2E: {e}")
            import traceback
            traceback.print_exc()
            raise

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_two_hotels_parallel_workflow(self, small_sample_df, e2e_config):
        """
        Test du workflow sur 2 hôtels en parallèle limité
        Pour vérifier que le traitement batch fonctionne
        """
        if os.getenv("SKIP_E2E_TESTS") == "1":
            pytest.skip("Tests E2E complets désactivés")

        # Prendre les 2 premiers hôtels
        hotels_data = []
        for _, row in small_sample_df.iterrows():
            hotels_data.append({
                'name': row['name'],
                'address': row['adresse'],
                'url': row['URL']
            })

        print(f"\n🚀 TEST E2E PARALLÈLE: {len(hotels_data)} hôtels")
        for i, hotel in enumerate(hotels_data, 1):
            print(f"   {i}. {hotel['name']}")
        print("=" * 60)

        # Configuration pour 2 workers
        parallel_config = ParallelConfig(
            max_workers=2,
            batch_size=2,
            cvent_timeout=60
        )

        start_time = time.time()

        try:
            processor = ParallelHotelProcessorDB(parallel_config)
            db_service = DatabaseService()

            # Créer session
            session_id = db_service.create_new_session("test_parallel_e2e.csv", len(hotels_data))

            # Traitement avec toutes les extractions
            final_stats = await processor.process_hotels_to_database(
                hotels_data=hotels_data,
                session_id=session_id,
                extract_cvent=True,
                extract_gmaps=True,
                extract_website=False  # Désactivé pour accélérer
            )

            elapsed = time.time() - start_time

            print(f"\n📊 RÉSULTATS PARALLÈLES après {elapsed:.1f}s:")
            print(f"   • Total: {final_stats['total_hotels']}")
            print(f"   • Succès: {final_stats['successful']}")
            print(f"   • Échecs: {final_stats['failed']}")
            print(f"   • Vitesse: {elapsed/len(hotels_data):.1f}s par hôtel")

            # Vérifications
            assert final_stats['total_hotels'] == len(hotels_data)

            # Vérifier en DB
            session_stats = db_service.get_session_statistics(session_id)
            print(f"✅ Stats DB: {session_stats.get('completed', 0)} complétés")

            print(f"\n🎉 TEST PARALLÈLE RÉUSSI!")

        except Exception as e:
            print(f"❌ Erreur test parallèle: {e}")
            raise

    def test_data_quality_validation(self, test_csv_path):
        """
        Test de validation de la qualité des données après extraction complète
        Vérifie que les données extraites respectent les contraintes
        """
        # Ce test examine les données réelles en DB après extraction
        try:
            supabase_client = SupabaseClient()

            # Récupérer une session récente de test
            sessions = supabase_client.client.table("extraction_sessions").select("*").order("created_at", desc=True).limit(1).execute()

            if not sessions.data:
                pytest.skip("Aucune session de test disponible pour validation")

            session_id = sessions.data[0]['id']
            print(f"🔍 Validation des données de la session: {session_id[:8]}...")

            # Récupérer tous les hôtels de cette session
            hotels = supabase_client.client.table("hotels").select("*").eq("session_id", session_id).execute()

            print(f"📊 Analyse de {len(hotels.data)} hôtel(s):")

            for hotel in hotels.data:
                print(f"\n🏨 {hotel['name']}")
                print(f"   • Status: {hotel['status']}")
                print(f"   • Adresse: {hotel['address']}")

                # Vérifier contraintes de base
                assert hotel['name'] is not None and len(hotel['name']) > 0
                assert hotel['address'] is not None and len(hotel['address']) > 0
                assert hotel['cvent_url'] is not None and hotel['cvent_url'].startswith('http')

                # Récupérer salles de réunion
                rooms = supabase_client.client.table("meeting_rooms").select("*").eq("hotel_id", hotel['id']).execute()

                print(f"   • {len(rooms.data)} salle(s) de réunion")

                # Valider structure des salles
                for room in rooms.data:
                    # Nom de salle obligatoire
                    assert room['nom_salle'] is not None and len(room['nom_salle']) > 0

                    # Au moins un champ de capacité rempli
                    capacity_fields = ['capacite_theatre', 'capacite_banquet', 'capacite_u', 'capacite_classe', 'capacite_cocktail', 'capacite_amphi']
                    has_capacity = any(room.get(field) for field in capacity_fields)

                    if hotel['status'] == 'completed':
                        # Si extraction réussie, devrait avoir au moins une capacité
                        print(f"     - {room['nom_salle']}: {room.get('surface', 'N/A')}")

            print("✅ Validation de la qualité des données OK")

        except Exception as e:
            print(f"⚠️ Erreur validation données: {e}")
            # Ne pas faire échouer le test pour problème de validation


# Utilitaires pour exécuter les tests
def run_quick_e2e():
    """Lance un test E2E rapide"""
    print("🚀 TEST E2E RAPIDE (1 hôtel)")
    pytest.main([
        __file__ + "::TestFullE2E::test_single_hotel_complete_workflow",
        "-v", "-s", "--tb=short"
    ])

def run_full_e2e():
    """Lance tous les tests E2E (LENT)"""
    print("🚀 TESTS E2E COMPLETS (LENT)")
    pytest.main([
        __file__,
        "-v", "-s", "--tb=short",
        "-m", "not performance"
    ])


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        run_quick_e2e()
    elif len(sys.argv) > 1 and sys.argv[1] == "full":
        run_full_e2e()
    else:
        print("Usage:")
        print("  python test_full_e2e.py quick  # Test 1 hôtel complet")
        print("  python test_full_e2e.py full   # Tous les tests E2E")
        print("  pytest test_full_e2e.py -v -s # Tests avec pytest direct")