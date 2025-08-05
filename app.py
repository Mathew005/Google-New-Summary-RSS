import os
import json
import time
import threading
import feedparser
import google.generativeai as genai
import sqlite3
from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv
from urllib.parse import quote_plus
from collections import deque # More efficient for queue operations
import ollama

# --- Load environment ---
load_dotenv()

# --- Config ---
DATABASE_FILE = "news.db"
PAGE_SIZE = 6
FEED_FETCH_COUNT = 30
CACHE_EXPIRATION_SECONDS = 900  # 15 minutes

AI_PROVIDER = os.getenv("AI_PROVIDER", "ollama")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3n")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_MODEL = os.getenv("GOOGLE_MODEL", "gemini-1.5-flash")

if AI_PROVIDER == "google" and GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# --- Flask App & Threading ---
app = Flask(__name__)
# --- NEW: In-Memory Priority Queue for on-screen articles ---
PRIORITY_QUEUE = deque()
PRIORITY_LOCK = threading.Lock() # To safely access the queue from multiple threads

# --- Database Setup and Helpers (Unchanged) ---
def get_db_connection():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
            source TEXT, summary_original TEXT, image_url TEXT, ai_summary TEXT,
            status TEXT NOT NULL, topic TEXT NOT NULL, fetch_timestamp REAL NOT NULL
        );
    ''')
    conn.commit()
    conn.close()
    print("✅ Database initialized.")

# --- AI Summarizer (Unchanged) ---
def get_ai_summary(prompt: str):
    # ... (function is unchanged)
    try:
        if AI_PROVIDER == "google":
            model = genai.GenerativeModel(GOOGLE_MODEL)
            response = model.generate_content(prompt)
            return response.text.strip()
        else:
            response = ollama.chat(model=OLLAMA_MODEL, messages=[{"role": "user", "content": prompt}])
            return response['message']['content'].strip()
    except Exception as e:
        print(f"AI Summarization Error ({AI_PROVIDER}): {e}")
        return f"Error: AI summarization failed. Provider: {AI_PROVIDER}."

# --- RSS Feed Parsing (Unchanged) ---
def fetch_and_cache_news(cache_key: str):
    # ... (function is unchanged)
    if cache_key == "__trending__": rss_url = "https://news.google.com/rss?hl=en-IN&gl=IN&ceid=IN:en"
    else: rss_url = f"https://news.google.com/rss/search?q={quote_plus(cache_key)}&hl=en-IN&gl=IN&ceid=IN:en"
    print(f"CACHE MISS/EXPIRED: Fetching fresh news for '{cache_key}'")
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM articles WHERE topic = ?", (cache_key,))
        feed = feedparser.parse(rss_url)
        for entry in feed.entries[:FEED_FETCH_COUNT]:
            source = entry.source.title if 'source' in entry else 'Unknown Source'
            image_url = entry.media_content[0].get('url') if 'media_content' in entry and entry.media_content else None
            conn.execute("""
                INSERT OR IGNORE INTO articles (link, title, source, summary_original, image_url, status, topic, fetch_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (entry.link, entry.title, source, entry.summary, image_url, "pending", cache_key, time.time()))
        conn.commit()
        cursor = conn.execute("SELECT * FROM articles WHERE topic = ? ORDER BY id DESC LIMIT ?", (cache_key, FEED_FETCH_COUNT))
        return [dict(row) for row in cursor.fetchall()]
    finally: conn.close()

# --- Flask Routes ---
@app.route("/")
def main_app():
    # ... (route is unchanged)
    model_name = f"Google {GOOGLE_MODEL}" if AI_PROVIDER == "google" else f"Ollama {OLLAMA_MODEL.title()}"
    filter_topics = ["Technology", "Cricket", "Business", "Bollywood", "Politics", "Stock Market"]
    return render_template("index.html", model_name=model_name, filter_topics=filter_topics)

@app.route("/get-news")
def get_news():
    """Serves news and updates the priority queue with on-screen articles."""
    query = request.args.get("q", "").strip()
    page = request.args.get("page", 1, type=int)
    cache_key = query if query else "__trending__"
    
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT fetch_timestamp FROM articles WHERE topic = ? ORDER BY id DESC LIMIT 1", (cache_key,))
        latest_article = cursor.fetchone()
        
        articles = []
        is_stale = not latest_article or (time.time() - latest_article['fetch_timestamp']) > CACHE_EXPIRATION_SECONDS

        if is_stale: articles = fetch_and_cache_news(cache_key)
        else:
            print(f"CACHE HIT (FRESH): Using stored news for '{cache_key}'")
            cursor = conn.execute("SELECT * FROM articles WHERE topic = ? ORDER BY id DESC", (cache_key,))
            articles = [dict(row) for row in cursor.fetchall()]

        start_index = (page - 1) * PAGE_SIZE
        end_index = start_index + PAGE_SIZE
        paginated_articles = articles[start_index:end_index]
        has_more = end_index < len(articles)

        # --- NEW: Update the Priority Queue ---
        with PRIORITY_LOCK:
            # When a user loads page 1, we reset and create a new priority list.
            # For subsequent pages (2, 3...), we append to it.
            if page == 1:
                PRIORITY_QUEUE.clear()
            
            for article in paginated_articles:
                if article['status'] == 'pending' and article['link'] not in PRIORITY_QUEUE:
                    PRIORITY_QUEUE.append(article['link'])
            print(f"Priority queue updated. Size: {len(PRIORITY_QUEUE)}")

        return jsonify({"articles": paginated_articles, "has_more": has_more})
    finally: conn.close()

# --- Background Threads ---
def background_summarizer():
    """Summarizer that checks a high-priority queue first."""
    time.sleep(10)
    while True:
        pending_article = None
        conn = get_db_connection()
        try:
            priority_link = None
            # --- NEW: Step 1 - Check Priority Queue ---
            with PRIORITY_LOCK:
                if PRIORITY_QUEUE:
                    priority_link = PRIORITY_QUEUE.popleft() # Get first item

            if priority_link:
                print(f"PRIORITY QUEUE: Found high-priority link: {priority_link[:50]}...")
                cursor = conn.execute("SELECT * FROM articles WHERE link = ? AND status = 'pending'", (priority_link,))
                pending_article = cursor.fetchone()
                if not pending_article:
                    print("...but it was already processed. Moving on.")
            
            # --- Step 2 - Fallback to general queue if no priority item was found/valid ---
            if not pending_article:
                cursor = conn.execute("SELECT * FROM articles WHERE status = 'pending' LIMIT 1")
                pending_article = cursor.fetchone()

            # --- Step 3 - Process the selected article (same as before) ---
            if pending_article:
                article_dict = dict(pending_article)
                print(f"Summarizing: {article_dict['title'][:70]}...")
                conn.execute("UPDATE articles SET status = 'in_progress' WHERE id = ?", (article_dict['id'],))
                conn.commit()

                prompt = (f"You are an expert news analyst. Summarize the following Indian news article "
                          f"in 2-3 concise, insightful sentences:\n\n"
                          f"Title: {article_dict['title']}\n"
                          f"Content: {article_dict.get('summary_original', 'No content available.')}")
                summary = get_ai_summary(prompt)

                if "Error:" in summary:
                    conn.execute("UPDATE articles SET status = 'error', ai_summary = ? WHERE id = ?", (summary, article_dict['id']))
                else:
                    conn.execute("UPDATE articles SET status = 'done', ai_summary = ? WHERE id = ?", (summary, article_dict['id']))
                conn.commit()
                time.sleep(2)
            else:
                time.sleep(15)
        finally:
            conn.close()

def startup_tasks():
    # ... (function is unchanged)
    init_db()
    print("🧹 Resetting any stale 'in_progress' articles to 'pending'...")
    conn = get_db_connection()
    try:
        conn.execute("UPDATE articles SET status = 'pending' WHERE status = 'in_progress'")
        conn.commit()
        print("✅ Stale summaries have been reset.")
    finally: conn.close()
    if AI_PROVIDER == "ollama":
        print("🔄 Warming up Ollama model in the background...")
        try: ollama.chat(model=OLLAMA_MODEL, messages=[{"role": "user", "content": "Hello"}]); print(f"✅ Ollama model '{OLLAMA_MODEL}' is ready.")
        except Exception as e: print(f"❌ Ollama warm-up failed: {e}")
    print("🚀 Background startup tasks complete.")

if __name__ == "__main__":
    threading.Thread(target=startup_tasks, daemon=True).start()
    threading.Thread(target=background_summarizer, daemon=True).start()
    
    print("🚀 Flask server starting immediately...")
    app.run(debug=True, use_reloader=False)