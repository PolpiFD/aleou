import csv

def save_to_csv(headers, data, filename):
    """Sauvegarde en CSV"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"💾 Données sauvées dans {filename}")