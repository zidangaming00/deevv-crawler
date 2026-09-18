import os
import networkx as nx
import libsql_client

def calculate_global_pagerank():
    url_db = os.environ.get("TURSO_DATABASE_URL")
    token_db = os.environ.get("TURSO_AUTH_TOKEN")
    
    if not url_db or not token_db:
        print("Error: Kredensial Turso tidak ditemukan di Environment Variables!")
        return

    print("Menghubungkan ke Turso...")
    client = libsql_client.create_client_sync(url=url_db, auth_token=token_db)
    
    try:
        print("Mengambil data relasi dari page_graph...")
        result = client.execute("SELECT source_url, target_url FROM page_graph")
        
        G = nx.DiGraph()
        for row in result.rows:
            G.add_edge(row[0], row[1])
            
        if len(G.nodes) == 0:
            print("Graf kosong. Belum ada data URL.")
            return

        print("Menghitung PageRank global (NetworkX)...")
        pr_scores = nx.pagerank(G, alpha=0.85, max_iter=100)
        
        print(f"Menyimpan skor untuk {len(pr_scores)} URL ke database...")
        update_queries = []
        for url, score in pr_scores.items():
            scaled_score = score * 1000 
            update_queries.append(
                libsql_client.Statement(
                    "UPDATE documents SET pagerank = ? WHERE url = ?",
                    [scaled_score, url]
                )
            )
            
        for i in range(0, len(update_queries), 100):
            client.batch(update_queries[i:i+100])
            
        print("PageRank global berhasil diperbarui.")
        
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    calculate_global_pagerank()
