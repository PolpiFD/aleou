def extract_data_popup(page):
    """Extraction robuste pour la popup (structure <table>) avec validation du mapping"""
    print("🎯 Mode POPUP - extraction robuste avec validation")
    
    # Attendre le chargement complet
    page.wait_for_selector('tbody tr', timeout=15000)
    page.wait_for_timeout(3000)  # Attendre le rendu
    
    # Diagnostic complet
    tbody_rows = page.locator('tbody tr')
    room_links = page.locator('tbody tr a[href*="meetingRoom"]')
    
    print(f"📊 Diagnostic: {tbody_rows.count()} lignes tbody")
    print(f"🏨 Liens de salles: {room_links.count()}")
    
    # Lister tous les noms pour vérifier
    all_room_names = []
    for i in range(room_links.count()):
        name = room_links.nth(i).inner_text().strip()
        all_room_names.append(name)
    print(f"🏨 Salles détectées: {all_room_names}")
    
    # 🔥 NOUVEAU: Extraire les headers réels depuis le HTML
    try:
        print("🔍 Extraction des headers réels depuis le HTML...")
        header_spans = page.locator('thead th span.break-words, thead th span[class*="text-neutral-80"]')
        
        real_headers = []
        for i in range(header_spans.count()):
            header_text = header_spans.nth(i).inner_text().strip()
            if header_text and len(header_text) > 1:
                real_headers.append(header_text)
        
        print(f"📋 Headers extraits du HTML: {real_headers}")
        
        # Construire headers finaux: toujours 'Salles de réunion' en premier + headers réels
        if real_headers:
            headers = ['Salles de réunion'] + real_headers[1:] if len(real_headers) > 1 else ['Salles de réunion'] + real_headers
            print(f"✅ Headers utilisés: {headers}")
        else:
            raise Exception("Aucun header extrait")
            
    except Exception as e:
        print(f"⚠️ Extraction headers échouée: {e}")
        print("🔧 Utilisation des headers fixes comme fallback")
        # Fallback sur headers fixes
        headers = ['Salles de réunion', 'Taille', 'Hauteur du plafond', 'Capacité max', 
                   'En U', 'En banquet', 'En cocktail', 'Théâtre', 'Salle de classe', 'Salle de conférence', 'Demi-lune']
        print(f"📋 Headers de fallback utilisés: {headers}")
    
    # Extraction ligne par ligne avec élimination des doublons
    data = []
    seen_rooms = set()  # Pour éviter les doublons
    
    for i in range(tbody_rows.count()):
        try:
            row = tbody_rows.nth(i)
            
            # 🔧 CORRECTION: Chercher le nom de salle avec ou sans lien
            room_name = ""
            
            # Option 1: Chercher le lien (structure classique)
            room_link = row.locator('a[href*="meetingRoom"]').first
            if room_link.count() > 0:
                room_name = room_link.inner_text().strip()
            else:
                # Option 2: Pas de lien - extraire de la première cellule
                first_cell = row.locator('td').first
                if first_cell.count() > 0:
                    room_name = first_cell.inner_text().strip()
                    # Nettoyer le nom (prendre la première ligne si plusieurs)
                    room_name = room_name.split('\n')[0].strip()
            
            # Si toujours pas de nom, ignorer cette ligne
            if not room_name:
                continue
            
            # Éviter les doublons
            if room_name in seen_rooms:
                print(f"⏭️  {room_name} (doublon ignoré)")
                continue
            seen_rooms.add(room_name)
            
            # Extraire toutes les cellules
            cells = row.locator('td')
            row_data = [room_name]  # Commencer par le nom
            
            # Parcourir les autres cellules (en ignorant la première qui contient le nom)
            for j in range(1, cells.count()):
                cell = cells.nth(j)
                cell_text = cell.inner_text().strip()
                
                # 🔧 CORRECTION: Nettoyer et filtrer le texte debug
                cell_text = clean_cell_text(cell_text)
                
                # Ajouter seulement si pas vide après nettoyage
                if cell_text and cell_text != "-":
                    row_data.append(cell_text)
                else:
                    row_data.append("-")
            
            # Ajuster à la longueur des headers
            while len(row_data) < len(headers):
                row_data.append("-")
            row_data = row_data[:len(headers)]
            
            data.append(row_data)
            print(f"✅ {len(data):2d}. {room_name}")
            
        except Exception as e:
            print(f"❌ Erreur ligne {i+1}: {e}")
    
    print(f"📊 FINAL: {len(data)} salles extraites sur {len(all_room_names)} détectées")
    
    # Debug si on en a manqué
    if len(data) != len(all_room_names):
        extracted_names = [row[0] for row in data]
        missing = set(all_room_names) - set(extracted_names)
        print(f"⚠️  Salles manquées: {missing}")
    
    return headers, data


# Fonction supprimée - on utilise des headers standardisés


def clean_cell_text(cell_text):
    """Nettoie le texte d'une cellule en supprimant le debug Playwright
    
    Args:
        cell_text (str): Texte brut de la cellule
        
    Returns:
        str: Texte nettoyé
    """
    
    if not cell_text or not isinstance(cell_text, str):
        return "-"
    
    # 🔧 CORRECTION: Filtrer le texte debug Playwright
    debug_patterns = [
        'Déplacement de 75 % vers',
        'Déplacement vers la',
        'Zoom avant',
        'Page précédente',
        'Page suivante',
        'Accueil',
        'Fin'
    ]
    
    # Diviser en lignes et filtrer
    lines = [line.strip() for line in cell_text.split('\n') if line.strip()]
    clean_lines = []
    
    for line in lines:
        # Ignorer les lignes qui contiennent du texte debug
        is_debug = any(pattern in line for pattern in debug_patterns)
        if not is_debug and len(line) > 0:
            clean_lines.append(line)
    
    if len(clean_lines) > 1:
        # Prendre la ligne principale (souvent la première)
        main_text = clean_lines[0]
        # Si c'est des dimensions, garder le sous-texte aussi
        if 'm²' in main_text or 'm2' in main_text:
            return ' '.join(clean_lines[:2])  # Garde taille + dimensions
        else:
            return main_text
    elif len(clean_lines) == 1:
        return clean_lines[0]
    else:
        return "-"