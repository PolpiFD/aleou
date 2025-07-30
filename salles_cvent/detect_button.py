def detect_button(page):
    """Détecte quel type d'interface est disponible et clique sur le bon bouton
    
    Returns:
        str: Type d'interface détectée
            - "popup": Interface popup avec bouton cliqué
            - "popup_direct": Interface popup déjà visible
            - "grid": Interface grid avec bouton cliqué  
            - "grid_direct": Interface grid déjà visible
            - None: Aucune interface détectée
    """
    
    # Option 1: Popup complète (structure <table>)
    popup_btn = page.locator("button:has-text('Afficher toutes les salles')")
    if popup_btn.count() > 0 and popup_btn.is_visible():
        popup_btn.click()
        print("✅ Popup 'Afficher toutes les salles' ouverte")
        
        # Attendre que la popup se charge (chercher les <tr>)
        try:
            page.wait_for_selector('tbody tr', timeout=10000)
            return "popup"
        except TimeoutError:
            print("⚠️ Popup non chargée, fallback vers interface normale")
    
    # Option 2: Interface normale avec grid (structure div+role)
    # Cibler spécifiquement le bon bouton
    try:
        # Priorité 1: Dans la zone du tableau
        table_btn = page.locator("#meetingRoomsTableWrapper button:has-text('Tout afficher')")
        if table_btn.count() > 0 and table_btn.is_visible():
            table_btn.click()
            print("✅ Bouton 'Tout afficher' (tableau) cliqué")
            page.wait_for_timeout(2000)
            return "grid"
    except:
        pass
    
    # Priorité 3: Essayer le 2ème bouton "Tout afficher" (souvent le bon)
    try:
        all_btns = page.locator("button:visible:has-text('Tout afficher')")
        if all_btns.count() > 1:
            all_btns.nth(1).click()  # 2ème bouton (index 1)
            print("✅ 2ème bouton 'Tout afficher' cliqué")
            page.wait_for_timeout(2000)
            return "grid"
    except:
        pass
    
    # Option 3: Vérifier si les données sont déjà visibles sans bouton
    print("🔍 Recherche de données déjà visibles...")
    
    # Vérifier l'interface GRID directe (structures anciennes et modernes)
    try:
        # 1. Structure ancienne (div avec roles)
        old_grid_rows = page.locator('.public_fixedDataTable_bodyRow')
        if old_grid_rows.count() > 0:
            first_row_name = old_grid_rows.first.locator('li.MeetingRoomsGrid__meetingRoomNameWithLink___u0ADd')
            if first_row_name.count() > 0:
                room_name = first_row_name.inner_text().strip()
                if room_name and len(room_name) > 2:
                    print(f"✅ Interface GRID ancienne déjà visible - {old_grid_rows.count()} salles détectées")
                    return "grid_direct"
        
        # 2. Structure moderne (table HTML normale avec classes spécifiques)
        modern_table = page.locator('table')
        if modern_table.count() > 0:
            table_rows = page.locator('tbody tr')
            if table_rows.count() > 0:
                # Vérifier les headers pour confirmer que c'est un grid de salles
                headers = page.locator('th, thead th, [role="columnheader"]')
                has_meeting_headers = False
                for i in range(min(5, headers.count())):  # Vérifier les 5 premiers headers
                    header_text = headers.nth(i).inner_text().lower()
                    if any(keyword in header_text for keyword in ['nom', 'taille', 'capacit', 'hauteur', 'salle']):
                        has_meeting_headers = True
                        break
                
                if has_meeting_headers:
                    print(f"✅ Interface GRID moderne déjà visible - {table_rows.count()} salles détectées")
                    return "grid_direct"
                    
    except Exception as e:
        print(f"⚠️ Erreur vérification GRID direct: {e}")
        pass
    
    # Vérifier l'interface POPUP directe (structure table)
    try:
        popup_rows = page.locator('tbody tr')
        if popup_rows.count() > 0:
            # Vérifier si c'est dans une table de salles de réunion
            table_container = page.locator('table')
            if table_container.count() > 0:
                # Chercher des indices de données de salles
                first_row_text = popup_rows.first.inner_text().strip()
                if first_row_text and any(keyword in first_row_text.lower() 
                                        for keyword in ['room', 'salle', 'conference', 'meeting']):
                    print(f"✅ Interface POPUP déjà visible (sans bouton) - {popup_rows.count()} lignes détectées")
                    return "popup_direct"
    except Exception as e:
        print(f"⚠️ Erreur vérification POPUP direct: {e}")
        pass
    
    print("❌ Aucune interface d'affichage détectée")
    
    # Diagnostic final - afficher les éléments trouvés pour debug
    try:
        print("\n🔧 DIAGNOSTIC:")
        all_buttons = page.locator("button:visible")
        print(f"  - Boutons visibles trouvés: {all_buttons.count()}")
        
        grid_elements = page.locator('.public_fixedDataTable_bodyRow')
        print(f"  - Éléments GRID trouvés: {grid_elements.count()}")
        
        popup_elements = page.locator('tbody tr')
        print(f"  - Éléments POPUP trouvés: {popup_elements.count()}")
        
        # Afficher les premiers boutons trouvés
        for i in range(min(3, all_buttons.count())):
            btn_text = all_buttons.nth(i).inner_text()[:50]
            print(f"    Bouton {i+1}: '{btn_text}'")
    
    except Exception as e:
        print(f"  ⚠️ Erreur diagnostic: {e}")
    
    return None


def diagnose_page_structure(page):
    """Fonction de diagnostic pour analyser la structure de la page
    
    Args:
        page: Page Playwright
        
    Returns:
        dict: Informations de diagnostic
    """
    
    diagnostic = {
        'buttons': [],
        'grid_elements': 0,
        'popup_elements': 0,
        'tables': 0,
        'meeting_room_elements': 0
    }
    
    try:
        # Analyser les boutons
        buttons = page.locator("button:visible")
        for i in range(min(10, buttons.count())):
            btn_text = buttons.nth(i).inner_text().strip()
            if btn_text:
                diagnostic['buttons'].append(btn_text[:100])
        
        # Compter les éléments
        diagnostic['grid_elements'] = page.locator('.public_fixedDataTable_bodyRow').count()
        diagnostic['popup_elements'] = page.locator('tbody tr').count()
        diagnostic['tables'] = page.locator('table').count()
        diagnostic['meeting_room_elements'] = page.locator('li.MeetingRoomsGrid__meetingRoomNameWithLink___u0ADd').count()
        
    except Exception as e:
        diagnostic['error'] = str(e)
    
    return diagnostic
