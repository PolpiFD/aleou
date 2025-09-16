"""
Module client Supabase pour la gestion de la base de données
Gère les connexions, transactions et opérations CRUD
"""

import os
from typing import Dict, List, Optional, Any
from supabase import create_client, Client
from dotenv import load_dotenv
import time
from functools import wraps
from datetime import datetime

load_dotenv()


class SupabaseError(Exception):
    """Exception personnalisée pour les erreurs Supabase"""
    pass


def retry_on_error(max_retries=3, delay=1):
    """Décorateur pour retry automatique en cas d'erreur"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                    else:
                        raise SupabaseError(f"Failed after {max_retries} attempts: {str(e)}")
            raise last_error
        return wrapper
    return decorator


class SupabaseClient:
    """Client Supabase avec gestion d'erreurs et retry logic"""

    def __init__(self):
        """Initialise le client Supabase"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")

        if not self.url or not self.key:
            raise SupabaseError(
                "Variables SUPABASE_URL et SUPABASE_KEY requises dans .env"
            )

        try:
            self.client: Client = create_client(self.url, self.key)
        except Exception as e:
            raise SupabaseError(f"Erreur connexion Supabase: {str(e)}")

    # ============ Sessions Management ============

    @retry_on_error(max_retries=3)
    def create_extraction_session(
        self,
        session_name: str,
        total_hotels: int,
        csv_filename: str = None
    ) -> str:
        """Crée une nouvelle session d'extraction

        Args:
            session_name: Nom de la session
            total_hotels: Nombre total d'hôtels
            csv_filename: Nom du fichier CSV source

        Returns:
            str: ID de la session créée
        """
        data = {
            "session_name": session_name,
            "total_hotels": total_hotels,
            "csv_filename": csv_filename,
            "status": "processing",
            "last_activity": datetime.now().isoformat()
        }

        result = self.client.table("extraction_sessions").insert(data).execute()
        return result.data[0]["id"]

    @retry_on_error(max_retries=3)
    def update_session_status(
        self,
        session_id: str,
        status: str,
        processed_hotels: int = None
    ):
        """Met à jour le statut d'une session

        Args:
            session_id: ID de la session
            status: Nouveau statut
            processed_hotels: Nombre d'hôtels traités
        """
        data = {
            "status": status,
            "last_activity": datetime.now().isoformat()
        }
        if processed_hotels is not None:
            data["processed_hotels"] = processed_hotels

        self.client.table("extraction_sessions").update(data).eq(
            "id", session_id
        ).execute()

    @retry_on_error(max_retries=3)
    def update_session_activity(self, session_id: str):
        """Met à jour uniquement le timestamp d'activité d'une session

        Args:
            session_id: ID de la session
        """
        data = {"last_activity": datetime.now().isoformat()}
        self.client.table("extraction_sessions").update(data).eq(
            "id", session_id
        ).execute()

    # ============ Hotels Management ============

    @retry_on_error(max_retries=3)
    def insert_hotel(
        self,
        session_id: str,
        name: str,
        address: str = None,
        cvent_url: str = None
    ) -> str:
        """Insère un hôtel dans la base

        Args:
            session_id: ID de la session
            name: Nom de l'hôtel
            address: Adresse
            cvent_url: URL Cvent

        Returns:
            str: ID de l'hôtel créé
        """
        data = {
            "session_id": session_id,
            "name": name,
            "address": address,
            "cvent_url": cvent_url,
            "extraction_status": "pending"
        }

        result = self.client.table("hotels").insert(data).execute()
        return result.data[0]["id"]

    @retry_on_error(max_retries=3)
    def update_hotel_status(
        self,
        hotel_id: str,
        status: str,
        interface_type: str = None,
        salles_count: int = None,
        error_message: str = None
    ):
        """Met à jour le statut d'extraction d'un hôtel

        Args:
            hotel_id: ID de l'hôtel
            status: Nouveau statut
            interface_type: Type d'interface détecté
            salles_count: Nombre de salles extraites
            error_message: Message d'erreur si échec
        """
        data = {"extraction_status": status}

        if interface_type:
            data["interface_type"] = interface_type
        if salles_count is not None:
            data["salles_count"] = salles_count
        if error_message:
            data["error_message"] = error_message

        self.client.table("hotels").update(data).eq("id", hotel_id).execute()

    # ============ Meeting Rooms Management ============

    def _clean_capacity_value(self, value: Any) -> Optional[int]:
        """Nettoie et convertit une valeur de capacité

        Args:
            value: Valeur à nettoyer

        Returns:
            Optional[int]: Valeur entière ou None
        """
        if value is None or value in ['-', '', 'nan', 'NaN']:
            return None

        try:
            # Si c'est déjà un nombre
            if isinstance(value, (int, float)):
                return int(value)

            # Si c'est une chaîne
            if isinstance(value, str):
                # Nettoyer les espaces et caractères spéciaux
                cleaned = value.strip().replace(' ', '')
                if cleaned in ['-', '', 'nan', 'NaN']:
                    return None
                return int(cleaned)
        except (ValueError, TypeError):
            return None

        return None

    @retry_on_error(max_retries=3)
    def insert_meeting_rooms(
        self,
        hotel_id: str,
        rooms_data: List[Dict[str, Any]]
    ) -> int:
        """Insère plusieurs salles de réunion pour un hôtel

        Args:
            hotel_id: ID de l'hôtel
            rooms_data: Liste des données de salles

        Returns:
            int: Nombre de salles insérées
        """
        if not rooms_data:
            return 0

        # Préparer les données avec nettoyage
        cleaned_rooms = []
        for room in rooms_data:
            cleaned_room = {
                "hotel_id": hotel_id,
                "nom_salle": room.get("nom_salle")
            }

            # Nettoyer et ajouter uniquement les champs non-null
            if "surface" in room:
                surface = room["surface"]
                if surface and surface not in ['-', '', 'nan']:
                    cleaned_room["surface"] = surface

            # Nettoyer les capacités numériques
            capacity_fields = [
                "capacite_theatre", "capacite_classe", "capacite_banquet",
                "capacite_cocktail", "capacite_u", "capacite_amphi"
            ]

            for field in capacity_fields:
                if field in room:
                    cleaned_value = self._clean_capacity_value(room[field])
                    if cleaned_value is not None:
                        cleaned_room[field] = cleaned_value

            cleaned_rooms.append(cleaned_room)

        # Insérer en batch
        result = self.client.table("meeting_rooms").insert(cleaned_rooms).execute()
        return len(result.data)

    # ============ Google Maps Data ============

    @retry_on_error(max_retries=3)
    def insert_gmaps_data(
        self,
        hotel_id: str,
        gmaps_data: Dict[str, Any]
    ):
        """Insère les données Google Maps pour un hôtel

        Args:
            hotel_id: ID de l'hôtel
            gmaps_data: Données Google Maps
        """
        # Nettoyer les données
        cleaned_data = {"hotel_id": hotel_id}

        # Mapping des champs
        field_mapping = {
            "name": "gmaps_name",
            "website": "gmaps_website",
            "averageRating": "gmaps_rating",
            "reviewCount": "gmaps_review_count",
            "phoneNumber": "gmaps_phone",
            "address": "gmaps_address",
            "category": "gmaps_category",
            "isClosed": "gmaps_is_closed",
            "sharableLink": "gmaps_sharable_link",
            "oloc": "gmaps_region",
            "headerImageUrl": "gmaps_image_url",
            "openingHours": "gmaps_opening_hours"
        }

        for source_field, db_field in field_mapping.items():
            if source_field in gmaps_data:
                value = gmaps_data[source_field]
                if value is not None and value != "":
                    cleaned_data[db_field] = value

        self.client.table("hotel_gmaps_data").insert(cleaned_data).execute()

    # ============ Website Data ============

    @retry_on_error(max_retries=3)
    def insert_website_data(
        self,
        hotel_id: str,
        website_data: Dict[str, Any]
    ):
        """Insère les données website pour un hôtel

        Args:
            hotel_id: ID de l'hôtel
            website_data: Données website extraites depuis Firecrawl/LLM
        """
        # Commencer avec l'ID de l'hôtel
        cleaned_data = {"hotel_id": hotel_id}

        # Mapping des champs selon le schéma SQL fourni
        field_mapping = {
            # Corrections des noms de colonnes problématiques
            'source': 'website_source',
            'hotel_website_title': 'website_title',
            'hotel_email': 'website_email',
            # Champs directs (même nom)
            'website_url': 'website_url',
            'website_description': 'website_description',
            'extraction_method': 'extraction_method',
            'website_phone': 'website_phone',
            'opening_hours': 'opening_hours',
            'price_range': 'price_range',
            'photos_urls': 'photos_urls',
            'photos_count': 'photos_count',
            'capacite_max': 'capacite_max',
            'nombre_chambre': 'nombre_chambre',
            'nombre_chambre_twin': 'nombre_chambre_twin',
            'nombre_etoile': 'nombre_etoile',
            'meeting_rooms_available': 'meeting_rooms_available',
            'meeting_rooms_count': 'meeting_rooms_count',
            'largest_room_capacity': 'largest_room_capacity',
            'summary': 'summary',
            'content_length': 'content_length',
            'images_found': 'images_found',
            'llm_fields_extracted': 'llm_fields_extracted'
        }

        # Ajouter tous les champs PR boolean
        pr_fields = [
            'pr_amphi', 'pr_hotel', 'pr_acces_facile', 'pr_banquet', 'pr_contact',
            'pr_room_nb', 'pr_lieu_atypique', 'pr_nature', 'pr_mer', 'pr_montagne',
            'pr_centre_ville', 'pr_parking', 'pr_restaurant', 'pr_piscine', 'pr_spa',
            'pr_wifi', 'pr_sun', 'pr_contemporaine', 'pr_acces_pmr', 'pr_visio',
            'pr_eco_label', 'pr_rooftop', 'pr_esat'
        ]
        for field in pr_fields:
            field_mapping[field] = field

        # Mapper les données reçues vers les colonnes DB
        for original_key, value in website_data.items():
            if value is not None and value != "" and value != []:
                # Obtenir le nom de colonne DB correct
                db_column = field_mapping.get(original_key)
                if db_column:
                    if db_column == "photos_urls" and isinstance(value, list):
                        cleaned_data[db_column] = value
                    elif db_column.startswith('pr_'):
                        # Champs PR sont des booleans - convertir automatiquement
                        cleaned_data[db_column] = self._convert_to_boolean(value)
                    else:
                        cleaned_data[db_column] = value

        # Insérer seulement si on a des données utiles
        if len(cleaned_data) > 1:  # Plus que juste hotel_id
            self.client.table("hotel_website_data").insert(cleaned_data).execute()

    def _convert_to_boolean(self, value) -> bool:
        """Convertit intelligemment une valeur vers un boolean pour les champs PR

        Args:
            value: La valeur à convertir (peut être string, int, bool, etc.)

        Returns:
            bool: True ou False selon la logique de conversion
        """
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            # Valeurs textuelles communes
            if value.lower() in ['true', '1', 'yes', 'oui', 'on']:
                return True
            elif value.lower() in ['false', '0', 'no', 'non', 'off']:
                return False
            # Si c'est un nombre en string
            try:
                num_value = float(value)
                return num_value > 0
            except (ValueError, TypeError):
                # Si ce n'est pas un nombre, considérer les strings non-vides comme True
                return len(value.strip()) > 0

        if isinstance(value, (int, float)):
            # Les nombres > 0 sont True, 0 ou négatifs sont False
            return value > 0

        # Pour tout autre type, considérer les valeurs non-nulles comme True
        return value is not None

    # ============ Query Methods ============

    @retry_on_error(max_retries=3)
    def get_session_progress(self, session_id: str) -> Dict[str, Any]:
        """Récupère les statistiques de progression d'une session

        Args:
            session_id: ID de la session

        Returns:
            Dict: Statistiques de progression
        """
        # Utiliser la vue SQL créée
        result = self.client.table("extraction_progress").select("*").eq(
            "session_id", session_id
        ).execute()

        if result.data:
            return result.data[0]
        return {}

    @retry_on_error(max_retries=3)
    def get_pending_hotels(
        self,
        session_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Récupère les hôtels en attente de traitement

        Args:
            session_id: ID de la session
            limit: Nombre max d'hôtels à récupérer

        Returns:
            List[Dict]: Liste des hôtels à traiter
        """
        result = self.client.table("hotels").select("*").eq(
            "session_id", session_id
        ).eq(
            "extraction_status", "pending"
        ).limit(limit).execute()

        return result.data

    @retry_on_error(max_retries=3)
    def mark_hotels_processing(self, hotel_ids: List[str]):
        """Marque plusieurs hôtels comme en cours de traitement

        Args:
            hotel_ids: Liste des IDs d'hôtels
        """
        self.client.table("hotels").update(
            {"extraction_status": "processing"}
        ).in_("id", hotel_ids).execute()

    # ============ Transaction Support ============

    def insert_hotel_with_rooms_transaction(
        self,
        hotel_data: Dict[str, Any],
        rooms_data: List[Dict[str, Any]],
        gmaps_data: Optional[Dict] = None,
        website_data: Optional[Dict] = None
    ) -> bool:
        """Insère un hôtel avec toutes ses données en transaction

        Args:
            hotel_data: Données de l'hôtel
            rooms_data: Liste des salles
            gmaps_data: Données Google Maps optionnelles
            website_data: Données website optionnelles

        Returns:
            bool: True si succès, False sinon
        """
        try:
            # 1. Mettre à jour le statut de l'hôtel
            hotel_id = hotel_data["id"]
            self.update_hotel_status(
                hotel_id,
                status="completed",
                interface_type=hotel_data.get("interface_type"),
                salles_count=len(rooms_data)
            )

            # 2. Insérer les salles de réunion
            if rooms_data:
                self.insert_meeting_rooms(hotel_id, rooms_data)

            # 3. Insérer les données Google Maps si disponibles
            if gmaps_data:
                self.insert_gmaps_data(hotel_id, gmaps_data)

            # 4. Insérer les données website si disponibles (NON-BLOQUANT)
            if website_data:
                try:
                    self.insert_website_data(hotel_id, website_data)
                except Exception as website_error:
                    # Ne pas faire échouer la transaction complète pour website
                    print(f"⚠️ Échec insertion website (non critique): {website_error}")
                    # Continuer avec le reste de la transaction

            return True

        except Exception as e:
            # En cas d'erreur, marquer l'hôtel comme échoué
            try:
                self.update_hotel_status(
                    hotel_id,
                    status="failed",
                    error_message=str(e)
                )
            except:
                pass
            raise SupabaseError(f"Transaction failed: {str(e)}")