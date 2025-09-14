"""
Script pour lancer les tests de production complets
Simule l'utilisation réelle avec le CSV fourni
"""

import os
import sys
import pandas as pd
import asyncio
from pathlib import Path
from datetime import datetime

# Ajouter les modules au path
sys.path.append(str(Path(__file__).parent.parent))

from modules.supabase_client import SupabaseClient, SupabaseError
from modules.database_service import DatabaseService
from modules.parallel_processor_db import ParallelHotelProcessorDB, ParallelConfig
from services.extraction_service_db import ExtractionServiceDB


def test_supabase_connection():
    """Test de base de la connexion Supabase"""
    print("🔌 Test de connexion Supabase...")

    try:
        client = SupabaseClient()
        print("✅ Client Supabase initialisé")

        # Test session
        session_id = client.create_extraction_session(
            "Test Production Connection",
            1,
            "test.csv"
        )
        print(f"✅ Session créée: {session_id[:8]}...")

        # Nettoyer
        client.client.table("extraction_sessions").delete().eq(
            "id", session_id
        ).execute()
        print("✅ Nettoyage OK")

        return True

    except SupabaseError as e:
        print(f"❌ Erreur Supabase: {e}")
        return False
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
        return False


def test_database_service():
    """Test du service de base de données"""
    print("\n🗄️ Test du service de base de données...")

    try:
        service = DatabaseService()
        print("✅ DatabaseService initialisé")

        # Test création session
        session_id = service.create_new_session("test_production.csv", 5)
        print(f"✅ Session créée via service: {session_id[:8]}...")

        # Test mapping
        headers = ['Salles de réunion', 'Taille', 'En U', 'Théâtre']
        rows = [['Salle Test', '30 m²', '15', '25']]
        mapped = service.map_cvent_data_to_db(headers, rows)

        print(f"✅ Mapping colonnes OK: {len(mapped)} salle(s)")
        print(f"   • Champs mappés: {list(mapped[0].keys())}")

        # Finaliser
        service.finalize_session(session_id, success=True)
        print("✅ Session finalisée")

        return True

    except Exception as e:
        print(f"❌ Erreur service DB: {e}")
        return False


def test_with_real_csv(max_hotels=3):
    """Test avec le CSV réel fourni"""
    print(f"\n📊 Test avec CSV réel ({max_hotels} hôtels max)...")

    try:
        # Charger le CSV
        csv_path = Path(__file__).parent / "test quelques hotels.csv"
        if not csv_path.exists():
            print(f"❌ CSV non trouvé: {csv_path}")
            return False

        df = pd.read_csv(csv_path)
        sample_df = df.head(max_hotels)

        print(f"✅ CSV chargé: {len(sample_df)} hôtels")
        for i, row in sample_df.iterrows():
            print(f"   • {row['name']} ({row['adresse']})")

        # Test avec service d'extraction
        service = ExtractionServiceDB()
        db_service = DatabaseService()

        # Créer session
        session_id = db_service.create_new_session(
            "test_real_csv.csv",
            len(sample_df)
        )

        # Préparer données
        hotels_data = []
        for _, row in sample_df.iterrows():
            hotels_data.append({
                'name': row['name'],
                'address': row['adresse'],
                'url': row['URL']
            })

        # Insérer en DB
        hotel_ids = db_service.prepare_hotels_batch(session_id, hotels_data)
        print(f"✅ {len(hotel_ids)} hôtels insérés en DB")

        # Test récupération stats
        stats = db_service.get_session_statistics(session_id)
        print(f"✅ Stats session: {stats.get('total_hotels', 0)} total")

        # Finaliser
        db_service.finalize_session(session_id, success=True)
        print("✅ Session finalisée avec succès")

        return True

    except Exception as e:
        print(f"❌ Erreur test CSV réel: {e}")
        return False


def test_parallel_processing_mock(max_hotels=3):
    """Test du traitement parallèle avec mock"""
    print(f"\n⚡ Test traitement parallèle (mock, {max_hotels} hôtels)...")

    try:
        from unittest.mock import patch

        # Mock des extractions
        def mock_cvent_extract(*args, **kwargs):
            hotel_name = args[0] if args else "Hotel Mock"
            return {
                'success': True,
                'data': {
                    'interface_type': 'grid',
                    'headers': ['Salles de réunion', 'Taille', 'En U', 'Théâtre'],
                    'rows': [
                        [f'Salle {hotel_name[:10]}', '40 m²', '18', '35']
                    ],
                    'salles_count': 1
                }
            }

        # Charger échantillon
        csv_path = Path(__file__).parent / "test quelques hotels.csv"
        df = pd.read_csv(csv_path)
        sample_df = df.head(max_hotels)

        hotels_data = []
        for _, row in sample_df.iterrows():
            hotels_data.append({
                'name': row['name'],
                'address': row['adresse'],
                'url': row['URL']
            })

        with patch('modules.cvent_extractor.extract_cvent_data', side_effect=mock_cvent_extract):
            # Configuration pour tests
            config = ParallelConfig(
                max_workers=2,
                batch_size=2,
                cvent_timeout=30
            )

            processor = ParallelHotelProcessorDB(config)
            db_service = DatabaseService()

            # Créer session
            session_id = db_service.create_new_session("test_parallel.csv", len(hotels_data))

            print(f"🔄 Démarrage traitement parallèle...")
            start_time = datetime.now()

            # Traitement
            final_stats = asyncio.run(
                processor.process_hotels_to_database(
                    hotels_data=hotels_data,
                    session_id=session_id,
                    extract_cvent=True,
                    extract_gmaps=False,
                    extract_website=False
                )
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"✅ Traitement parallèle terminé en {duration:.1f}s:")
            print(f"   • Total: {final_stats['total_hotels']}")
            print(f"   • Succès: {final_stats['successful']}")
            print(f"   • Échecs: {final_stats['failed']}")
            print(f"   • Session: {final_stats['session_id'][:8]}...")

            # Vérifier en DB
            stats = db_service.get_session_statistics(session_id)
            print(f"✅ Vérification DB: {stats.get('completed', 0)} complétés")

            return final_stats['failed'] == 0

    except Exception as e:
        print(f"❌ Erreur traitement parallèle: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Fonction principale des tests"""
    print("🚀 TESTS DE PRODUCTION - Architecture Supabase")
    print("=" * 55)

    # Vérifier configuration
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        print("❌ Variables SUPABASE_URL et SUPABASE_KEY requises dans .env")
        return False

    if "your-project" in supabase_url:
        print("❌ Veuillez configurer vos vraies clés Supabase")
        return False

    print(f"✅ Configuration détectée:")
    print(f"   • URL: {supabase_url[:30]}...")
    print(f"   • Key: {supabase_key[:30]}...")

    # Tests étape par étape
    tests_results = []

    # 1. Connexion Supabase
    tests_results.append(("Connexion Supabase", test_supabase_connection()))

    # 2. Service DB
    tests_results.append(("Service Database", test_database_service()))

    # 3. CSV réel
    tests_results.append(("CSV Réel", test_with_real_csv(max_hotels=3)))

    # 4. Traitement parallèle
    tests_results.append(("Traitement Parallèle", test_parallel_processing_mock(max_hotels=3)))

    # Résultats
    print("\n" + "=" * 55)
    print("📊 RÉSULTATS DES TESTS")
    print("=" * 55)

    passed = 0
    for test_name, result in tests_results:
        status = "✅ PASSÉ" if result else "❌ ÉCHEC"
        print(f"{test_name:<25} | {status}")
        if result:
            passed += 1

    print(f"\n🎯 BILAN: {passed}/{len(tests_results)} tests passés")

    if passed == len(tests_results):
        print("🎉 TOUS LES TESTS PASSÉS - Architecture prête pour production!")
        return True
    else:
        print("⚠️ Certains tests ont échoué - Vérifiez la configuration")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)