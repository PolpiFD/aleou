#!/bin/bash

# Script d'installation et configuration initiale du VPS
# À exécuter une seule fois sur le serveur

set -e

# Configuration
PROJECT_DIR="/opt/hotel-extractor"
SERVICE_USER="hotel-extractor"

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Mise à jour du système
update_system() {
    log_info "Mise à jour du système..."
    apt-get update && apt-get upgrade -y
    apt-get install -y curl wget git unzip
}

# Installation de Docker
install_docker() {
    log_info "Installation de Docker..."
    
    # Suppression des versions précédentes
    apt-get remove -y docker docker-engine docker.io containerd runc || true
    
    # Installation des dépendances
    apt-get install -y ca-certificates gnupg lsb-release
    
    # Détection de la distribution
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        CODENAME=$VERSION_CODENAME
    else
        log_error "Impossible de détecter la distribution"
        exit 1
    fi
    
    log_info "Distribution détectée: $OS $CODENAME"
    
    # Ajout de la clé GPG officielle de Docker
    mkdir -p /etc/apt/keyrings
    
    # Configuration spécifique selon la distribution
    if [ "$OS" = "debian" ]; then
        curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $CODENAME stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    elif [ "$OS" = "ubuntu" ]; then
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $CODENAME stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    else
        log_error "Distribution non supportée: $OS"
        exit 1
    fi
    
    # Installation de Docker
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    
    # Installation de Docker Compose (version standalone)
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    
    # Démarrage et activation de Docker
    systemctl start docker
    systemctl enable docker
}

# Création de l'utilisateur de service
create_service_user() {
    log_info "Création de l'utilisateur de service..."
    
    if ! id "$SERVICE_USER" &>/dev/null; then
        useradd -r -s /bin/bash -d "$PROJECT_DIR" "$SERVICE_USER"
        usermod -aG docker "$SERVICE_USER"
    fi
}

# Configuration des répertoires
setup_directories() {
    log_info "Configuration des répertoires..."
    
    mkdir -p "$PROJECT_DIR"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$PROJECT_DIR"
    
    # Création des répertoires de données
    mkdir -p "$PROJECT_DIR"/{outputs,cache,demo_output,test_outputs,logs}
    chown -R "$SERVICE_USER:$SERVICE_USER" "$PROJECT_DIR"
}

# Clonage du repository
clone_repository() {
    log_info "Clonage du repository..."
    
    su - "$SERVICE_USER" -c "
        if [ ! -d '$PROJECT_DIR/.git' ]; then
            git clone https://github.com/PolpiFD/aleou_scrapping.git $PROJECT_DIR
        else
            cd $PROJECT_DIR && git pull origin main
        fi
    "
}

# Configuration du firewall
setup_firewall() {
    log_info "Configuration du firewall..."
    
    ufw allow ssh
    ufw allow 80
    ufw allow 443
    ufw --force enable
}

# Configuration de logrotate
setup_logrotate() {
    log_info "Configuration de logrotate..."
    
    cat > /etc/logrotate.d/hotel-extractor << 'EOF'
/opt/hotel-extractor/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 hotel-extractor hotel-extractor
    postrotate
        /usr/bin/docker-compose -f /opt/hotel-extractor/docker-compose.yml restart 2>/dev/null || true
    endscript
}
EOF
}

# Configuration du service systemd
setup_systemd_service() {
    log_info "Configuration du service systemd..."
    
    cat > /etc/systemd/system/hotel-extractor.service << EOF
[Unit]
Description=Hotel Extractor Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$PROJECT_DIR
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
User=$SERVICE_USER
Group=$SERVICE_USER

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable hotel-extractor.service
}

# Configuration de la rotation automatique des logs Docker
setup_docker_logging() {
    log_info "Configuration des logs Docker..."
    
    cat > /etc/docker/daemon.json << 'EOF'
{
    "log-driver": "json-file",
    "log-opts": {
        "max-size": "10m",
        "max-file": "3"
    }
}
EOF

    systemctl restart docker
}

# Vérification finale
verify_installation() {
    log_info "Vérification de l'installation..."
    
    docker --version
    docker-compose --version
    
    if systemctl is-enabled docker >/dev/null; then
        log_info "Docker est configuré pour démarrer automatiquement"
    else
        log_error "Problème avec la configuration Docker"
        exit 1
    fi
}

# Fonction principale
main() {
    if [ "$EUID" -ne 0 ]; then
        log_error "Ce script doit être exécuté en tant que root"
        exit 1
    fi
    
    log_info "Début de l'installation du VPS..."
    
    update_system
    install_docker
    create_service_user
    setup_directories
    setup_firewall
    setup_logrotate
    setup_systemd_service
    setup_docker_logging
    verify_installation
    
    log_info "Installation terminée!"
    log_warn "N'oubliez pas de:"
    log_warn "1. Configurer les secrets GitHub Actions"
    log_warn "2. Créer le fichier .env dans $PROJECT_DIR"
    log_warn "3. Modifier le Caddyfile avec votre domaine"
    log_warn "4. Rendre le repository public ou configurer l'authentification SSH"
}

main "$@"