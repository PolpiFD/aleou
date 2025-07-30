#!/bin/bash

# Script de déploiement pour VPS
# Usage: ./deploy.sh [production|development]

set -e

# Configuration par défaut
ENVIRONMENT=${1:-development}
PROJECT_NAME="aleou-extractor"
COMPOSE_FILE="docker-compose.yml"

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Vérification des prérequis
check_prerequisites() {
    log_info "Vérification des prérequis..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker n'est pas installé"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose n'est pas installé"
        exit 1
    fi
    
    if [ ! -f ".env" ]; then
        log_warn "Fichier .env non trouvé, création à partir du template..."
        cp .env.template .env
        log_warn "Veuillez configurer le fichier .env avant de continuer"
        exit 1
    fi
}

# Arrêt des services existants
stop_services() {
    log_info "Arrêt des services existants..."
    
    # Arrêter et supprimer tous les containers liés au projet
    docker-compose down --remove-orphans --volumes || true
    
    # Nettoyage des containers orphelins avec les noms spécifiques
    docker container rm -f aleou-app aleou-caddy 2>/dev/null || true
    
    # Attendre que les containers soient complètement arrêtés
    sleep 2
}

# Construction et démarrage des services
start_services() {
    log_info "Construction et démarrage des services..."
    
    if [ "$ENVIRONMENT" == "production" ]; then
        log_info "Déploiement en mode production avec Caddy..."
        docker-compose --profile production up -d --build
    else
        log_info "Déploiement en mode développement..."
        docker-compose up -d --build
    fi
}

# Vérification du statut des services
check_services() {
    log_info "Vérification du statut des services..."
    sleep 10
    
    if docker-compose ps | grep -q "Up"; then
        log_info "Services démarrés avec succès!"
        docker-compose ps
        
        if [ "$ENVIRONMENT" == "production" ]; then
            log_info "Application accessible sur le port 80/443"
        else
            log_info "Application accessible sur http://localhost:8501"
        fi
    else
        log_error "Erreur lors du démarrage des services"
        docker-compose logs
        exit 1
    fi
}

# Nettoyage des images inutilisées
cleanup() {
    log_info "Nettoyage des images Docker inutilisées..."
    docker image prune -f
}

# Mise à jour du code depuis Git
update_code() {
    log_info "Mise à jour du code depuis GitHub..."
    
    if git rev-parse --git-dir > /dev/null 2>&1; then
        git pull origin main
        log_info "Code mis à jour depuis GitHub"
    else
        log_warn "Pas dans un repository Git, skip mise à jour"
    fi
}

# Fonction principale
main() {
    log_info "Début du déploiement en mode $ENVIRONMENT..."
    
    check_prerequisites
    update_code
    stop_services
    start_services
    check_services
    cleanup
    
    log_info "Déploiement terminé avec succès!"
}

# Gestion des signaux
trap 'log_error "Déploiement interrompu"; exit 1' INT TERM

# Exécution du script principal
main "$@"