import json
import os
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests

# Daftar situs awal yang mau dirayapi (Seed URLs) - fokus ke sumber lokal/berkualitas
SEED_URLS = [
    "https://id.wikipedia.org/wiki/Halaman_Utama",
    "https://developer.mozilla.org/id/",
]

# Fungsi untuk membersihkan teks dan membuat token pencarian (Inverted Index sederhana)
def tokenize_text(text):
    # Ubah ke lowercase, ambil kata alfanumerik saja
    words = re.findall(r"\b[a-z0-9à-öø-ÿ]+\b", text.lower())
    # Buat kamus frekuensi kata & posisinya
    tokens = {}
    for pos, word in enumerate(words):
        # Abaikan kata yang terlalu pendek (<= 2 huruf)
        if len(word) > 2:
            if word not in tokens:
                tokens[word] = {"frequency": 0, "positions": []}
            tokens[word]["frequency"] += 1
            tokens[word]["positions"].append(pos)
    return tokens

def crawl_page(url):
    headers = {
        "User-Agent": "DeevvBot/1.0 (+https://github.com/ming00/deevv)"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None, []
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 1. Ambil Judul
        title_tag = soup.find("title")
        title = title_tag.get_text().strip() if title_tag else "No Title"
        
        # 2. Ambil Snippet / Deskripsi Meta
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            snippet = meta_desc.get("content").strip()
        else:
            # Fallback ambil teks paragraf pertama jika meta description tidak ada
            p_tag = soup.find("p")
            snippet = p_tag.get_text().strip()[:160] if p_tag else ""

        # 3. Ambil Favicon menggunakan Google S2 Favicon Service
        parsed_uri = urlparse(url)
        domain = parsed_uri.netloc
        favicon_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=32"

        # 4. Ambil teks bersih dari body untuk di-tokenisasi
        for script in soup(["script", "style", "nav", "footer"]):
            script.extract()
        body_text = soup.get_text(separator=" ")
        tokens = tokenize_text(body_text)

        # 5. Kumpulkan Link Keluar (untuk ditemukan crawler berikutnya)
        new_links = []
        for a_tag in soup.find_all("a", href=True):
            absolute_url = urljoin(url, a_tag["href"])
            # Pastikan hanya mengambil link http/https dari domain yang sama atau publik
            if absolute_url.startswith("http"):
                new_links.append(absolute_url)

        # Struktur Data Dokumen Utuh (Format ala DDG/Custom JSON)
        document_data = {
            "url": url,
            "domain": domain,
            "metadata": {
                "title": title,
                "snippet": snippet,
                "favicon_url": favicon_url,
            },
            "search_index_tokens": tokens
        }

        return document_data, list(set(new_links))[:10] # Ambil maksimal 10 link baru per halaman

    except Exception as e:
        print(f"Gagal merayapi {url}: {e}")
        return None, []

def main():
    visited = set()
    queue = list(SEED_URLS)
    database_results = []

    # Batasi misalnya hanya merayapi 5 halaman dulu untuk uji coba di GitHub Actions
    max_pages = 5
    count = 0

    while queue and count < max_pages:
        current_url = queue.pop(0)
        if current_url in visited:
            continue

        print(f"[{count+1}/{max_pages}] Merayapi: {current_url}")
        visited.add(current_url)

        doc_data, extracted_links = crawl_page(current_url)
        if doc_data:
            database_results.append(doc_data)
            count += 1
            for link in extracted_links:
                if link not in visited:
                    queue.append(link)

    # Simpan hasil ke file JSON
    os.makedirs("output", exist_ok=True)
    output_file = "output/deevv_index.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(database_results, f, ensure_ascii=False, indent=2)
    
    print(f"Berhasil! Data disimpan ke {output_file}")

if __name__ == "__main__":
    main()
