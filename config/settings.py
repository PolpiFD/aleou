"""
Configuration centralisée pour l'application de scraping d'hôtels
Utilise les variables d'environnement du fichier .env existant
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Charger le .env existant
load_dotenv()

@dataclass
class APIConfig:
    """Configuration des APIs externes"""
    openai_api_key: str
    google_maps_api_key: str
    firecrawl_api_key: str
    openai_model: str = "gpt-4.1-nano"
    
    def __post_init__(self):
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY manquant dans .env")
        if not self.google_maps_api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY manquant dans .env")
        if not self.firecrawl_api_key:
            print("⚠️ FIRECRAWL_API_KEY manquant - Firecrawl désactivé")

@dataclass
class ScrapingConfig:
    """Configuration du scraping"""
    playwright_headless: bool = True
    playwright_timeout: int = 25000
    max_retry_attempts: int = 2
    use_stealth_mode: bool = True
    rotate_user_agents: bool = True
    use_proxy_simulation: bool = True

@dataclass
class RateLimitConfig:
    """Configuration du rate limiting"""
    google_maps_rate_limit: int = 10
    openai_rate_limit: int = 5
    base_delay: float = 0.1
    max_delay: float = 5.0

@dataclass
class ParallelConfig:
    """Configuration du traitement parallèle"""
    max_workers: int = 4
    chunk_size: int = 10
    batch_size: int = 50

@dataclass
class CacheConfig:
    """Configuration du cache"""
    enable_cache: bool = True
    cache_ttl: int = 3600  # 1 heure
    redis_url: Optional[str] = None
    cache_file: str = "cache/gmaps_cache.json"

class Settings:
    """Configuration globale de l'application"""
    
    def __init__(self):
        self.api = APIConfig(
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-nano"),
            google_maps_api_key=os.getenv("GOOGLE_MAPS_API_KEY", ""),
            firecrawl_api_key=os.getenv("FIRECRAWL_API_KEY", "")
        )
        
        self.scraping = ScrapingConfig(
            playwright_headless=os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true",
            playwright_timeout=int(os.getenv("PLAYWRIGHT_TIMEOUT", "25000")),
            max_retry_attempts=int(os.getenv("MAX_RETRY_ATTEMPTS", "2"))
        )
        
        self.rate_limit = RateLimitConfig(
            google_maps_rate_limit=int(os.getenv("GOOGLE_MAPS_RATE_LIMIT", "10")),
            openai_rate_limit=int(os.getenv("OPENAI_RATE_LIMIT", "5")),
            base_delay=float(os.getenv("BASE_DELAY", "0.1")),
            max_delay=float(os.getenv("MAX_DELAY", "5.0"))
        )
        
        self.parallel = ParallelConfig(
            max_workers=int(os.getenv("MAX_PARALLEL_WORKERS", "4")),
            chunk_size=int(os.getenv("CHUNK_SIZE", "10")),
            batch_size=int(os.getenv("BATCH_SIZE", "50"))
        )
        
        self.cache = CacheConfig(
            enable_cache=os.getenv("ENABLE_CACHE", "true").lower() == "true",
            cache_ttl=int(os.getenv("CACHE_TTL", "3600")),
            redis_url=os.getenv("REDIS_URL"),
            cache_file=os.getenv("CACHE_FILE", "cache/gmaps_cache.json")
        )
    
    def validate(self) -> bool:
        """Valide que toutes les configurations requises sont présentes"""
        try:
            valid = True
            
            if not self.api.openai_api_key:
                print("❌ OPENAI_API_KEY manquant dans .env")
                valid = False
            if not self.api.google_maps_api_key:
                print("❌ GOOGLE_MAPS_API_KEY manquant dans .env")
                valid = False
            
            # Firecrawl optionnel
            if not self.api.firecrawl_api_key:
                print("⚠️ FIRECRAWL_API_KEY manquant - mode Legacy activé")
            
            if valid:
                print("✅ Configuration validée avec succès")
            return valid
        except Exception as e:
            print(f"❌ Erreur validation configuration: {e}")
            return False
    
    def print_summary(self):
        """Affiche un résumé de la configuration"""
        print("🔧 CONFIGURATION ACTUELLE:")
        print(f"   API OpenAI: {'✓' if self.api.openai_api_key else '✗'} (Modèle: {self.api.openai_model})")
        print(f"   API Google Maps: {'✓' if self.api.google_maps_api_key else '✗'}")
        print(f"   API Firecrawl: {'✓' if self.api.firecrawl_api_key else '✗'}")
        print(f"   Rate Limits: GMaps={self.rate_limit.google_maps_rate_limit}/s, OpenAI={self.rate_limit.openai_rate_limit}/s")
        print(f"   Workers parallèles: {self.parallel.max_workers}")
        print(f"   Cache: {'activé' if self.cache.enable_cache else 'désactivé'} (TTL: {self.cache.cache_ttl}s)")
        print(f"   Playwright: {'headless' if self.scraping.playwright_headless else 'visible'}")
        print(f"   Mode extraction: {'Firecrawl' if self.api.firecrawl_api_key else 'Legacy LLM'}")

# Instance globale
settings = Settings()

# Validation au chargement - ne pas échouer si Firecrawl manque
if not settings.validate():
    print("⚠️ Configuration incomplète - certaines fonctionnalités peuvent être désactivées")
    # Ne pas lever d'erreur pour permettre le fonctionnement en mode Legacy