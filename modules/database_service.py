"""
Service de base de données pour l'orchestration des opérations Supabase
Gère la logique métier et les transactions complexes
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import logging
import pandas as pd
import io
from .supabase_client import SupabaseClient, SupabaseError

# Configuration du logging
logger = logging.getLogger(__name__)


class DatabaseService:
    """Service de haut niveau pour les opérations de base de données"""

    # Mapping des colonnes Cvent vers les colonnes DB
    COLUMN_MAPPING = {
        # Variations pour surface
        'Taille': 'surface',
        'Taille de la salle': 'surface',
        'salle_taille': 'surface',

        # Variations pour théâtre
        'Théâtre': 'capacite_theatre',
        'théâtre': 'capacite_theatre',
        'capacite_theatre': 'capacite_theatre',

        # Variations pour classe
        'Salle de classe': 'capacite_classe',
        'salle de classe': 'capacite_classe',
        'capacite_classe': 'capacite_classe',

        # Variations pour banquet
        'En banquet': 'capacite_banquet',
        'en banquet': 'capacite_banquet',
        'capacite_banquet': 'capacite_banquet',

        # Variations pour cocktail
        'En cocktail': 'capacite_cocktail',
        'en cocktail': 'capacite_cocktail',
        'capacite_cocktail': 'capacite_cocktail',

        # Variations pour U
        'En U': 'capacite_u',
        'en u': 'capacite_u',
        'capacite_en_u': 'capacite_u',

        # Variations pour amphi
        'Amphithéâtre': 'capacite_amphi',
        'Amphi': 'capacite_amphi',
        'amphithéâtre': 'capacite_amphi',
        'amphi': 'capacite_amphi',

        # Nom de la salle
        'Salles de réunion': 'nom_salle',
        'Nom': 'nom_salle',
        'nom': 'nom_salle',
        'salle_nom': 'nom_salle',

        # Capacité maximum (on ne garde pas dans la DB car redondant)
        'Capacité maximum': None,
        'Capacité max': None,
        'capacite_maximum': None,
    }

    def __init__(self):
        """Initialise le service de base de données"""
        try:
            self.client = SupabaseClient()
            logger.info("Service de base de données initialisé")
        except SupabaseError as e:
            logger.error(f"Erreur initialisation service DB: {e}")
            raise

    def create_new_session(
        self,
        csv_filename: str,
        total_hotels: int
    ) -> str:
        """Crée une nouvelle session d'extraction

        Args:
            csv_filename: Nom du fichier CSV
            total_hotels: Nombre total d'hôtels

        Returns:
            str: ID de la session créée
        """
        session_name = f"Extraction {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        try:
            session_id = self.client.create_extraction_session(
                session_name=session_name,
                total_hotels=total_hotels,
                csv_filename=csv_filename
            )
            logger.info(f"Session créée: {session_id} pour {total_hotels} hôtels")
            return session_id
        except Exception as e:
            logger.error(f"Erreur création session: {e}")
            raise

    def prepare_hotels_batch(
        self,
        session_id: str,
        hotels_data: List[Dict[str, str]]
    ) -> List[str]:
        """Prépare un batch d'hôtels dans la DB

        Args:
            session_id: ID de la session
            hotels_data: Liste des données d'hôtels

        Returns:
            List[str]: Liste des IDs d'hôtels créés
        """
        hotel_ids = []

        for hotel in hotels_data:
            try:
                hotel_id = self.client.insert_hotel(
                    session_id=session_id,
                    name=hotel.get('name', 'Unknown'),
                    address=hotel.get('address', ''),
                    cvent_url=hotel.get('url', '')
                )
                hotel_ids.append(hotel_id)
            except Exception as e:
                logger.error(f"Erreur insertion hôtel {hotel.get('name')}: {e}")
                continue

        logger.info(f"Batch préparé: {len(hotel_ids)} hôtels insérés")
        return hotel_ids

    def map_cvent_data_to_db(
        self,
        headers: List[str],
        rows_data: List[List[str]]
    ) -> List[Dict[str, Any]]:
        """Mappe les données Cvent vers le format DB

        Args:
            headers: Headers extraits de Cvent
            rows_data: Données des salles

        Returns:
            List[Dict]: Données mappées pour la DB
        """
        mapped_rooms = []

        for row in rows_data:
            room_data = {}

            for i, header in enumerate(headers):
                if i >= len(row):
                    continue

                # Obtenir le nom de colonne DB
                db_column = self.COLUMN_MAPPING.get(
                    header,
                    self.COLUMN_MAPPING.get(header.lower())
                )

                # Si pas de mapping ou mapping vers None, ignorer
                if db_column is None:
                    continue

                # Si pas dans le mapping, ignorer aussi
                if db_column == header and header not in [
                    'surface', 'capacite_theatre', 'capacite_classe',
                    'capacite_banquet', 'capacite_cocktail', 'capacite_u',
                    'capacite_amphi', 'nom_salle'
                ]:
                    continue

                # Ajouter la valeur
                value = row[i] if i < len(row) else None
                if value:
                    room_data[db_column] = value

            # S'assurer qu'on a au moins le nom de la salle
            if 'nom_salle' in room_data:
                mapped_rooms.append(room_data)
            elif row and row[0]:  # Fallback: premier élément = nom
                room_data['nom_salle'] = row[0]
                mapped_rooms.append(room_data)

        logger.info(f"Mappé {len(mapped_rooms)} salles depuis Cvent")
        return mapped_rooms

    def process_hotel_extraction(
        self,
        hotel_id: str,
        cvent_result: Optional[Dict] = None,
        gmaps_result: Optional[Dict] = None,
        website_result: Optional[Dict] = None
    ) -> bool:
        """Traite l'extraction complète d'un hôtel

        Args:
            hotel_id: ID de l'hôtel
            cvent_result: Résultat extraction Cvent
            gmaps_result: Résultat extraction Google Maps
            website_result: Résultat extraction Website

        Returns:
            bool: True si succès
        """
        try:
            # Préparer les données de l'hôtel
            hotel_update = {
                "id": hotel_id,
                "interface_type": None,
                "salles_count": 0
            }

            # Mapper les données Cvent si disponibles
            rooms_data = []
            if cvent_result and cvent_result.get('success'):
                hotel_update["interface_type"] = cvent_result['data'].get(
                    'interface_type'
                )

                # Mapper les données des salles
                headers = cvent_result['data'].get('headers', [])
                rows = cvent_result['data'].get('rows', [])

                if headers and rows:
                    rooms_data = self.map_cvent_data_to_db(headers, rows)
                    hotel_update["salles_count"] = len(rooms_data)

            # Préparer les données Google Maps
            gmaps_data = None
            if gmaps_result and gmaps_result.get('extraction_status') == 'success':
                gmaps_data = gmaps_result

            # Préparer les données Website
            website_data = None
            if website_result and website_result.get('success'):
                website_data = website_result.get('website_data')

            # Insérer tout en transaction
            success = self.client.insert_hotel_with_rooms_transaction(
                hotel_data=hotel_update,
                rooms_data=rooms_data,
                gmaps_data=gmaps_data,
                website_data=website_data
            )

            if success:
                logger.info(
                    f"Hôtel {hotel_id} traité: "
                    f"{len(rooms_data)} salles, "
                    f"GMaps: {gmaps_data is not None}, "
                    f"Website: {website_data is not None}"
                )

            return success

        except Exception as e:
            logger.error(f"Erreur traitement hôtel {hotel_id}: {e}")
            # Marquer comme échoué
            try:
                self.client.update_hotel_status(
                    hotel_id=hotel_id,
                    status="failed",
                    error_message=str(e)
                )
            except:
                pass
            return False

    def process_batch_results(
        self,
        batch_results: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """Traite les résultats d'un batch d'extractions

        Args:
            batch_results: Résultats consolidés du batch

        Returns:
            Tuple[int, int]: (nombre de succès, nombre d'échecs)
        """
        logger.info(f"🔍 DEBUT process_batch_results - {len(batch_results)} résultats à traiter")
        success_count = 0
        error_count = 0

        for i, result in enumerate(batch_results):
            logger.info(f"🔍 Traitement résultat {i+1}/{len(batch_results)}")
            try:
                # Récupérer l'ID de l'hôtel depuis le résultat
                # (sera ajouté par le parallel processor)
                hotel_id = result.get('hotel_id')
                logger.info(f"🔍 Hotel ID récupéré: {hotel_id}")
                if not hotel_id:
                    logger.warning("Résultat sans hotel_id, ignoré")
                    error_count += 1
                    continue

                # Traiter l'extraction avec protection supplémentaire
                logger.info(f"🔍 Début process_hotel_extraction pour hotel_id {hotel_id}")
                success = self.process_hotel_extraction(
                    hotel_id=hotel_id,
                    cvent_result=result.get('cvent_data'),
                    gmaps_result=result.get('gmaps_data'),
                    website_result=result.get('website_data')
                )
                logger.info(f"🔍 Fin process_hotel_extraction pour hotel_id {hotel_id}, success={success}")

                if success:
                    success_count += 1
                else:
                    error_count += 1

            except Exception as hotel_error:
                logger.error(f"💥 Erreur critique sur hôtel {result.get('hotel_id', 'UNKNOWN')}: {hotel_error}")
                error_count += 1

        logger.info(f"🔍 FIN process_batch_results: {success_count} succès, {error_count} échecs")
        return success_count, error_count

    def get_session_statistics(
        self,
        session_id: str
    ) -> Dict[str, Any]:
        """Récupère les statistiques d'une session

        Args:
            session_id: ID de la session

        Returns:
            Dict: Statistiques de la session
        """
        logger.info(f"🔍 DEBUT get_session_statistics pour session_id={session_id}")
        try:
            stats = self.client.get_session_progress(session_id)
            logger.info(f"🔍 Statistiques récupérées: {stats}")
            return stats
        except Exception as e:
            logger.error(f"🔍 Erreur récupération stats pour session_id={session_id}: {e}")
            return {}

    def _is_session_truly_inactive(self, session_data: Dict) -> bool:
        """Détermine si une session est réellement inactive basé sur last_activity

        Args:
            session_data: Données de la session depuis Supabase

        Returns:
            bool: True si la session est inactive depuis plus de 3 minutes
        """
        last_activity_str = session_data.get('last_activity')

        if not last_activity_str:
            # Pas de last_activity = session très ancienne ou créée avant la mise à jour
            # On considère comme inactive
            return True

        try:
            # Parser le timestamp ISO
            last_activity = datetime.fromisoformat(last_activity_str.replace('Z', '+00:00'))
            current_time = datetime.now()

            # Si c'est un timestamp UTC, on doit le comparer à UTC
            if '+' in last_activity_str or 'Z' in last_activity_str:
                from datetime import timezone
                current_time = datetime.now(timezone.utc)

            # Calculer la différence
            time_diff = current_time - last_activity
            inactive_minutes = time_diff.total_seconds() / 60

            # Session inactive si pas d'activité depuis plus de 3 minutes
            return inactive_minutes > 3.0

        except Exception as e:
            logger.warning(f"Erreur parsing last_activity: {e}")
            # En cas d'erreur, on considère comme inactive par sécurité
            return True

    def detect_and_fix_stuck_sessions(self):
        """Détecte et corrige les sessions bloquées/gelées"""
        try:
            # Récupérer les sessions 'processing' qui pourraient être gelées
            stuck_sessions = self.client.client.table("extraction_sessions")\
                .select("*")\
                .eq("status", "processing")\
                .execute()

            fixed_count = 0
            for session in stuck_sessions.data:
                session_id = session['id']
                session_name = session.get('session_name', 'N/A')

                # NOUVELLE LOGIQUE: Vérifier d'abord si la session est réellement inactive
                if not self._is_session_truly_inactive(session):
                    # Session encore active (activité récente), on ne la touche pas
                    logger.debug(f"Session {session_name} encore active - ignorée par watchdog")
                    continue

                # Si on arrive ici, la session est réellement inactive depuis >3min
                logger.info(f"Session inactive détectée: {session_name} (>3min sans activité)")

                # Vérifier les hôtels réels vs déclarés
                actual_hotels = self.client.client.table("hotels")\
                    .select("*")\
                    .eq("session_id", session_id)\
                    .execute()

                actual_count = len(actual_hotels.data)
                declared_count = session.get('total_hotels', 0)

                # Compter les hôtels terminés
                completed_hotels = [h for h in actual_hotels.data
                                   if h.get('extraction_status') == 'completed']

                # Cas 1: Tous les hôtels sont completed mais session encore processing
                if len(completed_hotels) == actual_count and actual_count > 0:
                    logger.warning(f"Session gelée détectée: {session_name} - {actual_count} hôtels completed mais session en processing")
                    self.finalize_session(session_id, success=True)
                    fixed_count += 1
                    logger.info(f"Session {session_name} auto-finalisée ({actual_count} hôtels)")

                # Cas 2: Session inactive + données partielles (au moins quelques hôtels traités)
                elif actual_count > 0:
                    logger.warning(f"Session incomplète inactive: {session_name} - {actual_count}/{declared_count} hôtels traités, finalisée avec données partielles")
                    self.finalize_session(session_id, success=True)  # Succès car on a des données
                    fixed_count += 1
                    logger.info(f"Session {session_name} finalisée avec données partielles ({actual_count}/{declared_count})")

                # Cas 3: Session inactive sans aucun hôtel traité
                else:
                    logger.warning(f"Session vide inactive: {session_name} - Aucun hôtel traité, marquée échouée")
                    self.finalize_session(session_id, success=False)
                    fixed_count += 1
                    logger.info(f"Session {session_name} marquée échouée (aucune donnée)")

            if fixed_count > 0:
                logger.info(f"Watchdog: {fixed_count} sessions gelées corrigées")

            return fixed_count

        except Exception as e:
            logger.error(f"Erreur watchdog sessions: {e}")
            return 0

    def finalize_session(
        self,
        session_id: str,
        success: bool = True,
        success_count: Optional[int] = None,
        error_count: Optional[int] = None
    ):
        """Finalise une session d'extraction

        Args:
            session_id: ID de la session
            success: Si la session s'est terminée avec succès
            success_count: Nombre d'hôtels traités avec succès (optionnel)
            error_count: Nombre d'hôtels en erreur (optionnel)
        """
        logger.info(f"🔍 DEBUT finalize_session pour session_id={session_id}, success={success}")
        try:
            # Récupérer le nombre réel d'hôtels dans la DB
            logger.info(f"🔍 Récupération des hôtels réels pour session {session_id}")
            hotels_query = self.client.client.table("hotels").select("*").eq("session_id", session_id).execute()
            hotels_data = hotels_query.data or []
            actual_count = len(hotels_data)
            logger.info(f"🔍 Nombre d'hôtels réels trouvés: {actual_count}")

            # Comptage des statuts directement depuis la table hôtels
            completed_in_db = sum(1 for hotel in hotels_data
                                  if hotel.get('extraction_status') == 'completed')
            failed_in_db = sum(1 for hotel in hotels_data
                               if hotel.get('extraction_status') == 'failed')

            def _ensure_int(value, fallback=0):
                if value is None:
                    return fallback
                try:
                    return max(int(value), 0)
                except (TypeError, ValueError):
                    return fallback

            computed_success = _ensure_int(success_count, fallback=completed_in_db)
            computed_errors = _ensure_int(error_count, fallback=failed_in_db)

            processed_hotels = computed_success + computed_errors
            if processed_hotels > actual_count:
                processed_hotels = actual_count

            pending_hotels = max(actual_count - processed_hotels, 0)

            logger.info(
                "🔍 Comptage final - success=%s, errors=%s, pending=%s",
                computed_success,
                computed_errors,
                pending_hotels
            )

            if success and actual_count > 0 and processed_hotels == 0:
                logger.warning(
                    "⚠️ Aucun hôtel comptabilisé malgré le succès annoncé - fallback sur total réel"
                )
                processed_hotels = actual_count
                pending_hotels = 0

            # Récupérer la session actuelle
            logger.info(f"🔍 Récupération des données de session {session_id}")
            session_data = self.client.client.table("extraction_sessions").select("*").eq("id", session_id).execute()
            if not session_data.data:
                logger.error(f"Session {session_id} introuvable")
                return
            logger.info(f"🔍 Données de session récupérées")

            current_session = session_data.data[0]
            declared_total = current_session.get('total_hotels', 0)
            logger.info(f"🔍 Total déclaré: {declared_total}, Total réel: {actual_count}")

            # Détecter les incohérences
            if actual_count != declared_total:
                logger.warning(f"Incohérence détectée: {actual_count} hôtels réels vs {declared_total} déclarés")
                # Corriger automatiquement en prenant la réalité
                status = "completed" if success and processed_hotels > 0 else "failed"
                logger.info(f"🔍 Mise à jour status session vers {status}")
                self.client.update_session_status(
                    session_id=session_id,
                    status=status,
                    processed_hotels=processed_hotels if processed_hotels > 0 else actual_count
                )
                logger.info(f"🔍 Status mis à jour, mise à jour total_hotels")
                # Mettre à jour le total pour correspondre à la réalité
                logger.info(f"🔍 AVANT UPDATE direct total_hotels={actual_count}")
                update_result = self.client.client.table("extraction_sessions").update({
                    'total_hotels': actual_count
                }).eq('id', session_id).execute()
                logger.info(f"🔍 APRES UPDATE direct total_hotels - SUCCESS")
                logger.info(f"🔍 Session {session_id} corrigée: {actual_count} hôtels réels")
            else:
                # Pas d'incohérence, finalisation normale
                status = "completed" if success else "failed"
                stats_processed = 0
                stats = {}
                if success_count is None or error_count is None:
                    logger.info(f"🔍 Finalisation normale, récupération des statistiques")
                    stats = self.get_session_statistics(session_id)
                    logger.info(f"🔍 Statistiques récupérées: {stats}")
                    stats_completed = _ensure_int(stats.get('completed'), fallback=0)
                    stats_failed = _ensure_int(stats.get('failed'), fallback=0)
                    stats_processed = stats_completed + stats_failed

                if stats_processed > 0:
                    logger.info(f"🔍 Processed calculé via stats: {stats_processed}")
                    processed_hotels = max(processed_hotels, min(stats_processed, actual_count))
                    pending_hotels = max(actual_count - processed_hotels, 0)

                if not success and processed_hotels == 0 and actual_count > 0:
                    processed_hotels = actual_count - pending_hotels

                logger.info(
                    f"🔍 Mise à jour status final vers {status}, processed={processed_hotels}"
                )
                self.client.update_session_status(
                    session_id=session_id,
                    status=status,
                    processed_hotels=processed_hotels
                )
                logger.info(f"🔍 Session {session_id} finalisée normalement: {status}")

            logger.info(f"🔍 FIN finalize_session pour session_id={session_id} - SUCCESS")
        except Exception as e:
            logger.error(f"🔍 ERREUR dans finalize_session pour session_id={session_id}: {e}")

    def get_batch_hotels_with_ids(
        self,
        session_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Récupère un batch d'hôtels avec leurs IDs

        Args:
            session_id: ID de la session
            limit: Nombre max d'hôtels

        Returns:
            List[Dict]: Liste des hôtels avec leurs IDs DB
        """
        try:
            hotels = self.client.get_pending_hotels(session_id, limit)

            # Marquer comme en cours de traitement
            if hotels:
                hotel_ids = [h['id'] for h in hotels]
                self.client.mark_hotels_processing(hotel_ids)

            return hotels
        except Exception as e:
            logger.error(f"Erreur récupération batch: {e}")
            return []

    def export_session_to_csv(
        self,
        session_id: str,
        include_empty_rooms: bool = False
    ) -> str:
        """Exporte toutes les données d'une session vers un CSV consolidé

        Args:
            session_id: ID de la session à exporter
            include_empty_rooms: Inclure les hôtels sans salles

        Returns:
            str: Contenu CSV formaté
        """
        try:
            # Récupérer tous les hôtels de la session
            hotels_query = (
                self.client.client
                .table("hotels")
                .select("*")
                .eq("session_id", session_id)
                .execute()
            )

            if not hotels_query.data:
                logger.warning(f"Aucun hôtel trouvé pour session {session_id}")
                return self._create_empty_csv()

            # Pour chaque hôtel, récupérer ses salles
            csv_rows = []

            for hotel in hotels_query.data:
                hotel_id = hotel['id']

                # Récupérer les salles de cet hôtel
                rooms_query = (
                    self.client.client
                    .table("meeting_rooms")
                    .select("*")
                    .eq("hotel_id", hotel_id)
                    .execute()
                )

                # Si pas de salles et qu'on n'inclut pas les hôtels vides
                if not rooms_query.data and not include_empty_rooms:
                    continue

                # Si pas de salles mais on inclut les hôtels vides
                if not rooms_query.data and include_empty_rooms:
                    csv_rows.append(self._create_csv_row(hotel, None))
                else:
                    # Une ligne par salle
                    for room in rooms_query.data:
                        csv_rows.append(self._create_csv_row(hotel, room))

            # Convertir en DataFrame puis CSV
            if not csv_rows:
                logger.info(f"Aucune donnée à exporter pour session {session_id}")
                return self._create_empty_csv()

            df = pd.DataFrame(csv_rows)

            # Trier par nom d'hôtel puis nom de salle
            df = df.sort_values(['hotel_name', 'nom_salle'], na_position='last')

            # Convertir en CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()

            logger.info(
                f"Export CSV session {session_id}: {len(csv_rows)} lignes, "
                f"{len(hotels_query.data)} hôtels"
            )

            return csv_content

        except Exception as e:
            logger.error(f"Erreur export CSV session {session_id}: {e}")
            return self._create_error_csv(str(e))

    def _create_csv_row(self, hotel: Dict, room: Optional[Dict] = None) -> Dict:
        """Crée une ligne CSV à partir des données hôtel/salle

        Args:
            hotel: Données de l'hôtel depuis Supabase
            room: Données de la salle (optionnel)

        Returns:
            Dict: Ligne formatée pour CSV
        """
        # Données de base de l'hôtel
        row = {
            'hotel_name': hotel.get('name', ''),
            'hotel_address': hotel.get('address', ''),
            'cvent_url': hotel.get('cvent_url', ''),
            'extraction_date': hotel.get('extraction_date', ''),
            'interface_type': hotel.get('interface_type', ''),
            'extraction_status': hotel.get('extraction_status', ''),
            'session_id': hotel.get('session_id', ''),
        }

        # Récupérer et ajouter données Google Maps si disponibles
        try:
            gmaps_data = self.client.client.table("hotel_gmaps_data").select("*").eq("hotel_id", hotel['id']).execute()
            if gmaps_data.data:
                gmaps_row = gmaps_data.data[0]
                row.update({
                    'gmaps_name': gmaps_row.get('gmaps_name', ''),
                    'gmaps_address': gmaps_row.get('gmaps_address', ''),
                    'gmaps_phone': gmaps_row.get('gmaps_phone', ''),
                    'gmaps_rating': gmaps_row.get('gmaps_rating', ''),
                    'gmaps_website': gmaps_row.get('gmaps_website', ''),
                })
            else:
                row.update({
                    'gmaps_name': '', 'gmaps_address': '', 'gmaps_phone': '',
                    'gmaps_rating': '', 'gmaps_website': ''
                })
        except:
            row.update({
                'gmaps_name': '', 'gmaps_address': '', 'gmaps_phone': '',
                'gmaps_rating': '', 'gmaps_website': ''
            })

        # Récupérer et ajouter données Website/LLM si disponibles
        try:
            website_data = self.client.client.table("hotel_website_data").select("*").eq("hotel_id", hotel['id']).execute()
            if website_data.data:
                website_row = website_data.data[0]
                row.update({
                    'website_url': website_row.get('website_url', ''),
                    'website_phone': website_row.get('website_phone', ''),
                    'website_email': website_row.get('website_email', ''),
                    'price_range': website_row.get('price_range', ''),
                    'nombre_chambre': website_row.get('nombre_chambre', ''),
                    'nombre_etoile': website_row.get('nombre_etoile', ''),
                    'parking': website_row.get('pr_parking', ''),
                    'restaurant': website_row.get('pr_restaurant', ''),
                    'spa': website_row.get('pr_spa', ''),
                    'wifi': website_row.get('pr_wifi', ''),
                })
            else:
                row.update({
                    'website_url': '', 'website_phone': '', 'website_email': '',
                    'price_range': '', 'nombre_chambre': '', 'nombre_etoile': '',
                    'parking': '', 'restaurant': '', 'spa': '', 'wifi': ''
                })
        except:
            row.update({
                'website_url': '', 'website_phone': '', 'website_email': '',
                'price_range': '', 'nombre_chambre': '', 'nombre_etoile': '',
                'parking': '', 'restaurant': '', 'spa': '', 'wifi': ''
            })

        # Données de la salle si disponible
        if room:
            row.update({
                'nom_salle': room.get('nom_salle', ''),
                'surface': room.get('surface', ''),
                'capacite_theatre': room.get('capacite_theatre', ''),
                'capacite_classe': room.get('capacite_classe', ''),
                'capacite_banquet': room.get('capacite_banquet', ''),
                'capacite_cocktail': room.get('capacite_cocktail', ''),
                'capacite_u': room.get('capacite_u', ''),
                'capacite_amphi': room.get('capacite_amphi', ''),
            })
        else:
            # Hôtel sans salles
            row.update({
                'nom_salle': '',
                'surface': '',
                'capacite_theatre': '',
                'capacite_classe': '',
                'capacite_banquet': '',
                'capacite_cocktail': '',
                'capacite_u': '',
                'capacite_amphi': '',
            })

        return row

    def _create_empty_csv(self) -> str:
        """Crée un CSV vide avec les headers"""
        headers = [
            'hotel_name', 'hotel_address', 'cvent_url', 'extraction_date',
            'interface_type', 'extraction_status', 'session_id',
            'gmaps_name', 'gmaps_address', 'gmaps_phone', 'gmaps_rating', 'gmaps_website',
            'website_url', 'website_phone', 'website_email', 'price_range',
            'nombre_chambre', 'nombre_etoile', 'parking', 'restaurant', 'spa', 'wifi',
            'nom_salle', 'surface', 'capacite_theatre', 'capacite_classe',
            'capacite_banquet', 'capacite_cocktail', 'capacite_u', 'capacite_amphi'
        ]

        df = pd.DataFrame(columns=headers)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        return csv_buffer.getvalue()

    def _create_error_csv(self, error_message: str) -> str:
        """Crée un CSV d'erreur avec un message"""
        error_data = [{
            'hotel_name': f'ERREUR: {error_message}',
            'hotel_address': '',
            'cvent_url': '',
            'extraction_date': datetime.now().isoformat(),
            'interface_type': '',
            'extraction_status': 'error',
            'session_id': '',
            'gmaps_name': '', 'gmaps_address': '', 'gmaps_phone': '',
            'gmaps_rating': '', 'gmaps_website': '',
            'website_url': '', 'website_phone': '', 'website_email': '', 'price_range': '',
            'nombre_chambre': '', 'nombre_etoile': '', 'parking': '', 'restaurant': '', 'spa': '', 'wifi': '',
            'nom_salle': '', 'surface': '', 'capacite_theatre': '',
            'capacite_classe': '', 'capacite_banquet': '', 'capacite_cocktail': '',
            'capacite_u': '', 'capacite_amphi': ''
        }]

        df = pd.DataFrame(error_data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        return csv_buffer.getvalue()

    def get_session_export_stats(self, session_id: str) -> Dict[str, Any]:
        """Récupère les statistiques d'une session pour l'export

        Args:
            session_id: ID de la session

        Returns:
            Dict: Statistiques détaillées
        """
        try:
            # Compter les hôtels
            hotels_count = (
                self.client.client
                .table("hotels")
                .select("id", count="exact")
                .eq("session_id", session_id)
                .execute()
            )

            # Compter les hôtels avec données
            hotels_with_data = (
                self.client.client
                .table("hotels")
                .select("id", count="exact")
                .eq("session_id", session_id)
                .eq("extraction_status", "completed")
                .execute()
            )

            # Compter les salles totales
            hotels_ids = [h['id'] for h in
                         self.client.client
                         .table("hotels")
                         .select("id")
                         .eq("session_id", session_id)
                         .execute().data]

            total_rooms = 0
            if hotels_ids:
                rooms_count = (
                    self.client.client
                    .table("meeting_rooms")
                    .select("id", count="exact")
                    .in_("hotel_id", hotels_ids)
                    .execute()
                )
                total_rooms = rooms_count.count or 0

            return {
                'session_id': session_id,
                'total_hotels': hotels_count.count or 0,
                'hotels_with_data': hotels_with_data.count or 0,
                'total_rooms': total_rooms,
                'export_ready': total_rooms > 0
            }

        except Exception as e:
            logger.error(f"Erreur stats export session {session_id}: {e}")
            return {
                'session_id': session_id,
                'total_hotels': 0,
                'hotels_with_data': 0,
                'total_rooms': 0,
                'export_ready': False,
                'error': str(e)
            }