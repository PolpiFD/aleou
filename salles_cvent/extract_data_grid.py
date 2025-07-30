import time

def clean_cell_text_grid(cell_text):
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
        'Fin',
        '←', '→', '↑', '↓', '+', '--'  # Caractères de navigation
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

def extract_data_grid(page):
    """Extraction pour l'interface grid avec gestion des structures anciennes et modernes"""
    print("🎯 Mode GRID - détection structure et extraction")
    
    # Détecter le type de structure
    old_grid_rows = page.locator('.public_fixedDataTable_bodyRow')
    modern_table_rows = page.locator('tbody tr')
    
    if old_grid_rows.count() > 0:
        print("📋 Structure GRID ancienne détectée")
        return extract_data_grid_old(page)
    elif modern_table_rows.count() > 0:
        print("📋 Structure TABLE moderne détectée")
        return extract_data_grid_modern(page)
    else:
        raise Exception("Aucune structure de données détectée")


def extract_data_grid_old(page):
    """Extraction pour l'ancienne interface grid avec pagination des colonnes"""
    print("🎯 Mode GRID ANCIEN - extraction avec pagination des colonnes")
    
    # Attendre que les lignes soient chargées
    page.wait_for_function(
        "() => document.querySelectorAll('.public_fixedDataTable_bodyRow').length > 0",
        timeout=15000
    )
    
    all_headers = []  # Ne pas ajouter manuellement 'Salles de réunion'
    all_data = {}  # Dict pour stocker toutes les données par nom de salle
    
    page_number = 1
    
    # ÉTAPE 1: Extraire toutes les colonnes visibles initialement
    print(f"📄 Page {page_number} - Extraction complète initiale...")
    
    # Headers initiaux
    initial_headers = []
    header_elements = page.locator('[role="columnheader"] .MeetingRoomsGrid__sortableHeaderCellName___2B7FS')
    for i in range(header_elements.count()):
        header_text = header_elements.nth(i).inner_text().strip()
        if header_text and len(header_text) > 1:
            initial_headers.append(header_text)
    
    # 🔧 CORRECTION: Standardiser les headers pour éviter les doublons
    all_headers = standardize_grid_headers(initial_headers)
    print(f"📋 Headers standardisés: {all_headers}")
    
    # Données initiales complètes
    all_data = extract_current_page_data(page)
    
    # ÉTAPE 2: Cliquer et extraire seulement la nouvelle colonne
    while True:
        # Vérifier s'il y a un bouton "Suivant" disponible
        next_button = page.locator('span[role="button"]:has(span[aria-label="Suivant"])')
        if next_button.count() > 0 and next_button.is_visible():
            page_number += 1
            print(f"\n➡️ Page {page_number} - Clic pour nouvelle colonne...")
            next_button.click()
            time.sleep(2)  # Attendre le chargement des nouvelles colonnes
            
            # Extraire les nouveaux headers pour identifier la nouvelle colonne
            new_headers = []
            header_elements = page.locator('[role="columnheader"] .MeetingRoomsGrid__sortableHeaderCellName___2B7FS')
            for i in range(header_elements.count()):
                header_text = header_elements.nth(i).inner_text().strip()
                if header_text and len(header_text) > 1:
                    new_headers.append(header_text)
            
            # 🔧 CORRECTION: Standardiser aussi les nouvelles colonnes
            new_column = new_headers[-1] if new_headers else None
            if new_column:
                standardized_new_column = standardize_single_header(new_column)
                if standardized_new_column not in all_headers:
                    all_headers.append(standardized_new_column)
                    print(f"📋 Nouvelle colonne standardisée: {new_column} -> {standardized_new_column}")
                
                # Extraire seulement la nouvelle colonne (dernière position)
                new_column_data = extract_new_column_data(page)
                
                # Ajouter la nouvelle colonne aux données existantes
                for room_name, new_value in new_column_data.items():
                    if room_name in all_data:
                        all_data[room_name].append(new_value)
                    else:
                        all_data[room_name] = [new_value]
            else:
                print("⚠️ Aucune nouvelle colonne détectée")
                
        else:
            print("✅ Plus de bouton 'Suivant' - extraction terminée")
            break
    
    # Validation des headers extraits (dynamiques selon l'hôtel)
    if len(all_headers) < 2:
        print("⚠️ Très peu de headers extraits, vérifier l'extraction")
        # Minimum requis : nom de salle + au moins une propriété
        if not all_headers:
            all_headers = ['Salles de réunion']
    
    # Convertir en format standard
    data = []
    for room_name, values in all_data.items():
        # Créer une ligne avec le nom en premier
        row = [room_name]
        
        # Ajouter toutes les autres valeurs
        if isinstance(values, list):
            # Si values est déjà une liste, on l'utilise directement (sauf le nom)
            other_values = [v for v in values if v != room_name]
            row.extend(other_values)
        else:
            # Si c'est une chaîne, l'ajouter directement
            row.append(values)
        
        # Ajuster longueur pour correspondre aux headers
        while len(row) < len(all_headers):
            row.append("-")
        row = row[:len(all_headers)]
        
        data.append(row)
        print(f"✅ {len(data):2d}. {room_name} | {len(row)-1} valeurs")
    
    print(f"\n🎉 Extraction terminée: {len(all_headers)} colonnes, {len(data)} salles")
    return all_headers, data


def extract_current_page_data(page):
    """Extrait les données de la page courante - extraction complète par ligne"""
    
    current_data = {}
    rows = page.locator('.public_fixedDataTable_bodyRow')
    
    for i in range(rows.count()):
        row = rows.nth(i)
        
        # Nom de la salle
        name_elem = row.locator('li.MeetingRoomsGrid__meetingRoomNameWithLink___u0ADd')
        name = name_elem.inner_text().strip() if name_elem.count() > 0 else f"Salle_{i+1}"
        
        if name not in current_data:
            current_data[name] = []
        
        # Extraire TOUTES les cellules de données (gridcell) dans l'ordre
        cells = row.locator('[role="gridcell"]')
        
        for j in range(cells.count()):
            cell = cells.nth(j)
            
            # Ignorer la première cellule qui contient le nom
            if j == 0:
                continue
                
            # Extraire le contenu de la cellule
            cell_text = ""
            
            # 1. Chercher les spans avec du texte
            spans = cell.locator('span:visible')
            if spans.count() > 0:
                span_texts = []
                for k in range(spans.count()):
                    span_content = spans.nth(k).inner_text().strip()
                    if span_content and span_content != name:
                        span_texts.append(span_content)
                if span_texts:
                    cell_text = ' '.join(span_texts)
            
            # 2. Chercher les capacités spécifiques 
            if not cell_text:
                capacities = cell.locator('div.MeetingRoomsGrid__capacityValue___84mjP:visible')
                if capacities.count() > 0:
                    cell_text = capacities.first.inner_text().strip()
            
            # 3. Fallback: texte général de la cellule
            if not cell_text:
                cell_text = cell.inner_text().strip().replace('\n', ' ')
                # Nettoyer si ça contient le nom de salle
                if name in cell_text:
                    cell_text = cell_text.replace(name, '').strip()
            
            # 🔧 CORRECTION: Nettoyer le texte et ignorer les cellules vides
            cell_text = clean_cell_text_grid(cell_text)
            
            if not cell_text or cell_text.strip() == "" or cell_text == "-":
                continue
                
            # Ajouter seulement les valeurs nettoyées et non-vides
            current_data[name].append(cell_text)
    
    return current_data


def extract_new_column_data(page):
    """Extrait seulement la nouvelle colonne (dernière à droite) après un clic"""
    
    new_column_data = {}
    rows = page.locator('.public_fixedDataTable_bodyRow')
    
    for i in range(rows.count()):
        row = rows.nth(i)
        
        # Nom de la salle
        name_elem = row.locator('li.MeetingRoomsGrid__meetingRoomNameWithLink___u0ADd')
        name = name_elem.inner_text().strip() if name_elem.count() > 0 else f"Salle_{i+1}"
        
        # Extraire l'AVANT-DERNIÈRE cellule (nouvelle colonne)
        cells = row.locator('[role="gridcell"]')
        if cells.count() > 1:
            last_cell = cells.nth(-2)  # Avant-dernière cellule = nouvelle colonne
            

            
            # Extraire le contenu de cette cellule
            cell_text = ""
            
            # 1. Chercher les spans avec du texte
            spans = last_cell.locator('span:visible')
            if spans.count() > 0:
                span_texts = []
                for k in range(spans.count()):
                    span_content = spans.nth(k).inner_text().strip()
                    if span_content and span_content != name:
                        span_texts.append(span_content)
                if span_texts:
                    cell_text = ' '.join(span_texts)
            
            # 2. Chercher les capacités spécifiques 
            if not cell_text:
                capacities = last_cell.locator('div.MeetingRoomsGrid__capacityValue___84mjP:visible')
                if capacities.count() > 0:
                    cell_text = capacities.first.inner_text().strip()
            
            # 3. Fallback: texte général de la cellule
            if not cell_text:
                cell_text = last_cell.inner_text().strip().replace('\n', ' ')
                if name in cell_text:
                    cell_text = cell_text.replace(name, '').strip()
            

            
            # 🔧 CORRECTION: Nettoyer le texte avant stockage
            cell_text = clean_cell_text_grid(cell_text)
            final_value = cell_text if cell_text and cell_text != "-" else "-"
            new_column_data[name] = final_value
    
    return new_column_data


def extract_data_grid_modern(page):
    """Extraction pour les tables HTML modernes (structure <table><tbody><tr>)"""
    print("🎯 Mode TABLE MODERNE - extraction directe")
    
    # Attendre que la table soit chargée
    page.wait_for_selector('tbody tr', timeout=15000)
    page.wait_for_timeout(2000)  # Attendre le rendu
    
    # 🔧 CORRECTION: Extraire et standardiser les headers
    raw_headers = []
    
    # Chercher les headers dans thead ou th
    header_selectors = ['thead th', 'th', '[role="columnheader"]']
    header_elements = None
    
    for selector in header_selectors:
        elements = page.locator(selector)
        if elements.count() > 0:
            header_elements = elements
            break
    
    if header_elements:
        for i in range(header_elements.count()):
            header_text = header_elements.nth(i).inner_text().strip()
            if header_text and header_text.lower() not in ['nom', 'name']:
                raw_headers.append(header_text)
    
    # Standardiser tous les headers
    headers = standardize_grid_headers(raw_headers)
    print(f"📋 Headers standardisés: {headers}")
    
    # 2. EXTRACTION DES DONNÉES
    data = []
    rows = page.locator('tbody tr')
    
    for i in range(rows.count()):
        try:
            row = rows.nth(i)
            
            # Extraire toutes les cellules
            cells = row.locator('td')
            row_data = []
            
            for j in range(cells.count()):
                cell = cells.nth(j)
                
                if j == 0:  # Première cellule = nom de la salle
                    # 🎯 CIBLER .font-medium pour le nom
                    name_element = cell.locator('.font-medium, [class*="font-medium"]').first
                    if name_element.count() > 0:
                        room_name = name_element.inner_text().strip()
                    else:
                        # Fallback: première ligne du texte
                        room_name = cell.inner_text().strip().split('\n')[0].strip()
                    row_data.append(room_name)
                else:
                    # Autres cellules : extraire le contenu principal
                    cell_text = cell.inner_text().strip()
                    
                    # 🔧 CORRECTION: Utiliser la fonction de nettoyage unifiée
                    cleaned_text = clean_cell_text_grid(cell_text)
                    row_data.append(cleaned_text)
            
            # Ajuster la longueur pour correspondre aux headers
            while len(row_data) < len(headers):
                row_data.append("-")
            row_data = row_data[:len(headers)]
            
            data.append(row_data)
            print(f"✅ {len(data):2d}. {row_data[0]} | {len(row_data)-1} valeurs")
            
        except Exception as e:
            print(f"⚠️ Erreur ligne {i+1}: {e}")
            continue
    
    print(f"\n🎉 Extraction moderne terminée: {len(headers)} colonnes, {len(data)} salles")
    return headers, data


def standardize_grid_headers(raw_headers):
    """Standardise les headers du GRID pour éviter les doublons avec POPUP
    
    Args:
        raw_headers (list): Headers bruts extraits
        
    Returns:
        list: Headers standardisés
    """
    
    standardized = ['Salles de réunion']  # Toujours en premier
    
    for header in raw_headers:
        standardized_header = standardize_single_header(header)
        if standardized_header not in standardized:
            standardized.append(standardized_header)
    
    return standardized


def standardize_single_header(header):
    """Standardise un header individuel
    
    Args:
        header (str): Header brut
        
    Returns:
        str: Header standardisé
    """
    
    if not header or not isinstance(header, str):
        return "unknown"
    
    header_clean = header.strip()
    
    # Mapping vers les standards POPUP (pour uniformité)
    mapping = {
        'Salles de réunion': 'Salles de réunion',
        'Taille de la salle': 'Taille',
        'Hauteur du plafond': 'Hauteur du plafond',
        'Capacité maximum': 'Capacité max',         # UNIFIER
        'Capacité maximale': 'Capacité max',        # UNIFIER  
        'En U': 'En U',
        'En banquet': 'En banquet', 
        'En cocktail': 'En cocktail',
        'Théâtre': 'Théâtre',
        'Salle de classe': 'Salle de classe',
        'Salle de conférence': 'Salle de conférence',
        'Demi-lune (Cabaret)': 'Demi-lune',
        'Cabaret': 'Demi-lune'
    }
    
    if header_clean in mapping:
        return mapping[header_clean]
    
    # Fallback: garder tel quel
    return header_clean