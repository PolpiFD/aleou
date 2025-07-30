"""
Rate Limiter intelligent pour les APIs externes
Gère les quotas et les délais d'attente pour éviter les erreurs 429
"""

import asyncio
import time
from typing import Dict, Any
from dataclasses import dataclass
import logging


@dataclass
class RateLimitConfig:
    """Configuration du rate limiting"""
    requests_per_minute: int = 60
    requests_per_second: int = 10
    burst_requests: int = 5
    cooldown_seconds: int = 60


class RateLimiter:
    """Rate limiter adaptatif pour APIs externes"""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.requests_history = []
        self.is_cooling_down = False
        self.cooldown_until = 0
        self.consecutive_errors = 0
        self.lock = asyncio.Lock()
    
    async def acquire(self) -> bool:
        """Acquiert une permission pour faire une requête
        
        Returns:
            bool: True si la requête peut être effectuée
        """
        # 🔧 FIX: Vérification rapide sans lock pour éviter la sérialisation
        current_time = time.time()
        
        # Vérifier cooldown sans lock (lecture simple)
        if self.is_cooling_down and current_time < self.cooldown_until:
            wait_time = self.cooldown_until - current_time
            if wait_time > 0:
                print(f"🛑 Rate limit: attente {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                # Reset cooldown après attente
                async with self.lock:
                    if current_time >= self.cooldown_until:
                        self.is_cooling_down = False
        
        # Lock minimal pour vérifier et calculer l'attente
        should_wait = False
        wait_time = 0
        
        async with self.lock:
            current_time = time.time()
            
            # Nettoyer l'historique (garder seulement la dernière minute)
            self._clean_history(current_time)
            
            # Vérifier les limites
            if self._should_wait():
                should_wait = True
                wait_time = self._calculate_wait_time()
        
        # Attendre HORS du lock pour permettre la parallélisation
        if should_wait and wait_time > 0:
            print(f"⏳ Rate limit: attente {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)
        
        # Lock minimal pour enregistrer la requête
        async with self.lock:
            self.requests_history.append(time.time())
            return True
    
    def _clean_history(self, current_time: float):
        """Nettoie l'historique des requêtes anciennes"""
        minute_ago = current_time - 60
        self.requests_history = [
            req_time for req_time in self.requests_history 
            if req_time > minute_ago
        ]
    
    def _should_wait(self) -> bool:
        """Détermine si on doit attendre avant la prochaine requête"""
        current_time = time.time()
        
        # Vérifier limite par minute
        if len(self.requests_history) >= self.config.requests_per_minute:
            return True
        
        # Vérifier limite par seconde (dernière seconde)
        recent_requests = [
            req_time for req_time in self.requests_history
            if req_time > current_time - 1
        ]
        
        if len(recent_requests) >= self.config.requests_per_second:
            return True
        
        return False
    
    def _calculate_wait_time(self) -> float:
        """Calcule le temps d'attente nécessaire"""
        current_time = time.time()
        
        # Si on dépasse la limite par minute
        if len(self.requests_history) >= self.config.requests_per_minute:
            oldest_request = min(self.requests_history)
            return max(0, 60 - (current_time - oldest_request))
        
        # Si on dépasse la limite par seconde
        recent_requests = [
            req_time for req_time in self.requests_history
            if req_time > current_time - 1
        ]
        
        if recent_requests:
            return max(0, 1 - (current_time - min(recent_requests)))
        
        return 0
    
    async def handle_error(self, status_code: int):
        """Gère les erreurs d'API et ajuste le rate limiting
        
        Args:
            status_code (int): Code de statut HTTP de l'erreur
        """
        async with self.lock:
            if status_code == 429:  # Too Many Requests
                self.consecutive_errors += 1
                cooldown_time = min(10 * (2 ** min(self.consecutive_errors, 3)), 60)  # Max 1 min
                
                self.is_cooling_down = True
                self.cooldown_until = time.time() + cooldown_time
                
                print(f"🚨 Rate limit dépassé! Cooldown {cooldown_time}s...")
            
            elif 500 <= status_code < 600:  # Server errors
                self.consecutive_errors += 1
                
                # 🔧 FIX: Cooldowns réduits pour éviter la paralysie
                if self.consecutive_errors >= 5:
                    # Après 5 erreurs consécutives, très court cooldown (API probablement down)
                    cooldown_time = 5
                    print(f"⚠️ API probablement en panne ({status_code}) - Cooldown minimal {cooldown_time}s...")
                else:
                    # Cooldowns progressifs mais raisonnables
                    cooldown_time = min(5 * self.consecutive_errors, 15)  # Max 15s
                    print(f"⚠️ Erreur serveur {status_code}! Attente {cooldown_time}s...")
                
                self.is_cooling_down = True  
                self.cooldown_until = time.time() + cooldown_time
    
    def reset_errors(self):
        """Remet à zéro le compteur d'erreurs consécutives"""
        self.consecutive_errors = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques du rate limiter"""
        current_time = time.time()
        recent_requests = [
            req_time for req_time in self.requests_history
            if req_time > current_time - 60
        ]
        
        return {
            'requests_last_minute': len(recent_requests),
            'requests_limit_minute': self.config.requests_per_minute,
            'consecutive_errors': self.consecutive_errors,
            'is_cooling_down': self.is_cooling_down,
            'cooldown_remaining': max(0, self.cooldown_until - current_time) if self.is_cooling_down else 0
        }


class APIRateLimitManager:
    """Gestionnaire centralisé des rate limiters pour différentes APIs"""
    
    def __init__(self):
        self.limiters = {}
    
    def get_limiter(self, api_name: str, config: RateLimitConfig = None) -> RateLimiter:
        """Récupère ou crée un rate limiter pour une API
        
        Args:
            api_name (str): Nom de l'API (ex: 'google_maps')
            config (RateLimitConfig): Configuration spécifique
            
        Returns:
            RateLimiter: Instance du rate limiter
        """
        if api_name not in self.limiters:
            if config is None:
                config = RateLimitConfig()  # Configuration par défaut
            
            self.limiters[api_name] = RateLimiter(config)
        
        return self.limiters[api_name]
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Retourne les statistiques de tous les rate limiters"""
        return {
            api_name: limiter.get_stats()
            for api_name, limiter in self.limiters.items()
        }


# Instance globale pour partager entre modules
rate_limit_manager = APIRateLimitManager() 