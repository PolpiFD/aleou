# Documentation Solution d'Extraction Hôtelière

## Vue d'ensemble de la solution

Notre solution automatise l'extraction complète d'informations hôtelières en combinant trois technologies complémentaires. Elle permet de traiter des milliers d'établissements et d'extraire automatiquement les données des salles de réunion, les informations générales de l'hôtel depuis google maps ainsi que des informations complémentaires sur le site web de l'hôtel.

Le système fonctionne en traitant un fichier CSV contenant une liste d'hôtels avec leurs URLs Cvent. Pour chaque hôtel, la solution va automatiquement récupérer les données depuis trois sources différentes et les consolider dans un fichier final.

## Extraction des données Cvent avec serveur headless

Pour extraire les informations des salles de réunion depuis Cvent, nous utilisons un serveur headless avec la technologie Playwright. Cette approche simule un navigateur web réel qui navigue automatiquement sur les pages Cvent de chaque hôtel.

Le serveur headless est nécessaire car l'API de Cvent a été modifiée en cours de route ne permettent plus une extraction simple. Notre système détecte automatiquement le type d'interface (grille ou popup) et s'adapte pour extraire toutes les données : nom des salles, capacités selon différentes configurations (banquet, théâtre, en U), dimensions, et hauteur sous plafond.

## Enrichissement avec Google Maps

En parallèle de l'extraction Cvent, le système utilise l'API Google Maps pour enrichir chaque hôtel avec des informations supplémentaires. En recherchant l'hôtel par son nom et son adresse, nous récupérons automatiquement les notes clients, le nombre d'avis, les coordonnées GPS exactes, et surtout l'URL du site web officiel.

Cette étape s'accompagne d'un coût API assez élevé au vu du nombre d'information a extraire, tableau des tarifs ci-dessous : 

| SKU | Quota gratuit / mois | 0‑100 000 appels | 100 001‑500 000 | ≥ 500 001 | Source |
| --- | --- | --- | --- | --- | --- |
| **Place Details Essentials** (champs de base : nom, adresse, coordonnées…) | 10 000 | **5 $/1 000** | 4 $/1 000 | 3 $/1 000 | ([Google for Developers](https://developers.google.com/maps/billing-and-pricing/pricing)) |
| **Place Details Enterprise** (champs rating, userRatingCount, priceLevel, phone, horaires…) | 1 000 | **20 $/1 000** | 16 $/1 000 | 12 $/1 000 | ([Google for Developers](https://developers.google.com/maps/billing-and-pricing/pricing)) |
| **Place Details Enterprise + Atmosphere** (reviews complets, résumé éditorial, etc.) | 1 000 | **25 $/1 000** | 20 $/1 000 | 15 $/1 000 | ([Google for Developers](https://developers.google.com/maps/billing-and-pricing/pricing)) |

## Extraction intelligente avec Firecrawl

Pour extraire les données des sites web d'hôtels, nous utilisons Firecrawl, une solution spécialisée qui combine scraping avancé et intelligence artificielle. Cette technologie nous permet de contourner toutes les protections mises en place par les grandes chaînes hôtelières (Cloudflare, protection anti-bot, etc.).

Firecrawl analyse automatiquement le contenu de chaque site web et extrait de manière structurée : le nombre de chambres, les services disponibles (parking, restaurant, spa, wifi), les informations de contact, et jusqu'à 15 images pertinentes par hôtel. L'IA essaie au maximum de filtrer automatiquement les images pour ne garder que celles montrant les chambres, espaces communs, restaurant, et extérieur, en excluant les logos et éléments de navigation.

Après de nombreux tests comparatifs, Firecrawl s'est révélé être la solution la plus fiable du marché, justifiant son coût de 99$/mois par sa capacité à traiter efficacement même les sites les plus complexes.

## Évolution de la recherche web

Initialement, nous avions implémenté un algorithme de recherche Google automatique pour trouver les sites web d'hôtels. Cependant, cette approche s'est révélée trop lente et peu pertinente dans ses résultats. Les temps de traitement étaient excessifs et les sites trouvés n'étaient pas toujours les bons (très rarement a vrai dire).

Nous avons donc décidé de nous appuyer exclusivement sur les sites web fournis par Google Maps, qui sont vérifiés et correspondent réellement aux établissements recherchés. Cette décision nous a permis de supprimer l'abonnement Autom.dev (environ 50$/mois) et d'améliorer significativement les performances globales.

## Limitations actuelles et performances

**Rate limiting Firecrawl** : Avec le plan Firecrawl actuel, nous sommes contraints à 10 extractions par minute pour respecter les limites de notre abonnement. Cela signifie qu'un traitement de 600 hôtels prend environ une heure. Cette limitation peut être supprimée en passant au plan supérieur (même tarif de 99$/mois) qui autorise jusqu'à 100 extractions par minute, ce qui diviserait par 10 les temps de traitement pour les gros volumes.

Cette restriction temporaire nous permet néanmoins de traiter efficacement des volumes moyens tout en maintenant la stabilité du système et le respect des quotas API.

## Résultats et bénéfices

La solution finale produit un fichier CSV consolidé où chaque ligne représente une salle de réunion avec toutes les métadonnées de l'hôtel correspondant. Les utilisateurs obtiennent également les URLs directes des images, les informations de contact complètes, et les données de géolocalisation.

Le temps de traitement est divisé par 10 par rapport aux méthodes manuelles, et la qualité des données est considérablement améliorée avec 40% de champs supplémentaires renseignés par rapport à l'ancienne version.