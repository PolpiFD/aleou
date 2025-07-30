# 🚀 Aleou Extractor

**Extracteur d'informations hôtelières automatisé** avec Firecrawl, OpenAI et Google Maps.

[![Deploy Status](https://img.shields.io/badge/deploy-automated-brightgreen)](https://github.com/YourUsername/aleou-extractor/actions)

## 🎯 Fonctionnalités

### ✅ Actuellement disponible
- **Extraction Cvent** : Extraction des salles de conférence et leurs capacités
- **Interface Streamlit** : Interface web intuitive pour les extractions
- **Support CSV** : Traitement par lot de plusieurs hôtels
- **URL unique** : Extraction d'un seul hôtel
- **Consolidation CSV** : Un seul fichier consolidé avec tous les hôtels et leurs salles
- **Interface optimisée** : Gestion de gros volumes (6000+ hôtels) avec affichage simplifié
- **Gestion des interfaces directes** : Support des hôtels sans bouton d'affichage

### 🚧 En développement
- Informations Google Maps (ratings, reviews, coordonnées)
- Recherche automatique du site web officiel
- Extraction de photos depuis les sites d'hôtels

## 🚀 Installation

1. **Cloner le projet**
```bash
git clone <repository>
cd script_aleou
```

2. **Installer les dépendances**
```bash
pip install -r requirements.txt
```

3. **Installer Playwright (pour l'extraction Cvent)**
```bash
playwright install chromium
```

## 📋 Utilisation

### Lancer l'interface
```bash
streamlit run main.py
```

L'interface sera accessible sur `http://localhost:8501`

### Mode CSV (multiple hôtels)

1. Préparez un fichier CSV avec les colonnes **exactes** :
   - `name` : Nom de l'hôtel
   - `adresse` : Adresse complète
   - `URL` : URL Cvent de l'hôtel

**Exemple de fichier CSV :**
```csv
name,adresse,URL
Hôtel nhow Brussels,Rue Royale 250 Brussels,https://www.cvent.com/venues/fr-FR/brussels/hotel/nhow-brussels-bloom/venue-01915c66-916c-499a-9675-e4a5c3f7ebbf
```

2. Uploadez le fichier dans l'interface
3. Sélectionnez les types d'extraction souhaités
4. Lancez l'extraction

### Mode URL unique

1. Remplissez le formulaire :
   - Nom de l'hôtel
   - Adresse (optionnel)
   - URL Cvent
2. Sélectionnez les extractions
3. Lancez l'extraction

## 📁 Structure du projet

```
script_aleou/
├── main.py                    # Interface Streamlit principale
├── modules/
│   ├── cvent_extractor.py     # Module d'extraction Cvent
│   ├── gmaps_extractor.py     # [À venir] Google Maps
│   ├── website_finder.py      # [À venir] Recherche site web
│   └── photo_extractor.py     # [À venir] Extraction photos
├── salles_cvent/              # Code d'extraction Cvent existant
│   ├── detect_button.py       # Détection interface Cvent
│   ├── extract_data_grid.py   # Extraction interface grille
│   ├── extract_data_popup.py  # Extraction interface popup
│   └── save_to_csv.py         # Sauvegarde CSV
├── outputs/                   # Fichiers de sortie générés
├── exemple_hotels.csv         # Fichier d'exemple
└── requirements.txt           # Dépendances Python
```

## 📊 Formats de sortie

### Extraction Cvent
Les données extraites sont sauvegardées en CSV avec les colonnes dynamiques selon l'hôtel :
- `Salles de réunion` : Nom de la salle
- `Taille de la salle` : Surface en m²
- `Hauteur du plafond` : Hauteur en mètres
- `Dimensions de la salle` : Dimensions détaillées
- `Capacité maximum` : Capacité totale
- `En U`, `En banquet`, `En cocktail`, etc. : Capacités par configuration

### Fichiers générés
- **Fichiers individuels** : `salles_grid_HotelName_YYYYMMDD_HHMMSS.csv`
- **Fichier consolidé** : `hotels_consolidation_YYYYMMDD_HHMMSS.csv` (TOUS les hôtels)
- **Statistiques** : `consolidation_stats_YYYYMMDD_HHMMSS.txt`
- **Dossier de sortie** : `outputs/`

### Structure du CSV consolidé
Le fichier consolidé contient une ligne par salle avec les métadonnées de l'hôtel :
```csv
hotel_name,hotel_address,salle_nom,salle_taille,hauteur_plafond,dimensions,capacite_maximum,capacite_en_u,capacite_banquet,...
Hôtel A,Adresse A,Salle 1,50m2,2.3m,8x6,30,20,25
Hôtel A,Adresse A,Salle 2,80m2,2.3m,10x8,50,30,40
Hôtel B,Adresse B,Salle 1,40m2,2.5m,6x7,25,15,20
```

## ⚠️ Prérequis et limitations

### URLs Cvent supportées
- Seules les URLs contenant `cvent.com` sont acceptées
- L'hôtel doit avoir un onglet "Espace de réunion" accessible
- Les interfaces supportées : Grid et Popup

### Performance
- **Extraction séquentielle** : Une URL à la fois (pour l'instant)
- **Temps moyen** : 15-30 secondes par hôtel
- **Mode headless** : Navigateur invisible pour les performances
- **Gros volumes** : Interface simplifiée pour 6000+ hôtels
- **Consolidation automatique** : Un seul fichier final, pas un par hôtel
- **Prochainement** : Parallélisation pour traitement simultané

### Gestion d'erreurs
- Validation automatique des URLs
- Gestion des timeouts (30s par page)
- Affichage détaillé des erreurs d'extraction

## 🔧 Configuration avancée

### Variables d'environnement
Aucune configuration particulière requise pour l'extraction Cvent.

### Customisation
- Timeout d'extraction : Modifiable dans `modules/cvent_extractor.py`
- Dossier de sortie : Paramétrable via l'interface

## 📞 Support

Pour des questions ou des problèmes :
1. Vérifiez que l'URL Cvent est correcte et accessible
2. Consultez les logs dans la console pour plus de détails
3. Vérifiez que l'hôtel a bien un onglet "Espace de réunion"

## 🚀 Roadmap

- [ ] **Google Maps API** : Extraction ratings, reviews, coordonnées
- [ ] **Website Finder** : Recherche automatique site officiel
- [ ] **Photo Extractor** : Download images depuis sites hôtels
- [ ] **Consolidation** : Fusion toutes les données en un seul CSV
- [ ] **Export formats** : Support Excel, JSON
- [ ] **Authentification** : Gestion des accès utilisateurs 