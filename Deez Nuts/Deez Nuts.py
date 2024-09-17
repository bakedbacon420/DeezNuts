import sys
import os
import json
import time
import subprocess
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from urllib.parse import urlparse
import random
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import requests

try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
except ImportError:
    print("Spotipy is not installed. Please install it using: pip install spotipy")
    sys.exit(1)

from deezer import API

# Set up logging
logging.basicConfig(filename='spotify_deezer_downloader.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_url(url, allowed_domains):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and any(domain in result.netloc for domain in allowed_domains)
    except ValueError:
        return False

class ArtistNode:
    def __init__(self, spotify_id, name, depth=0):
        self.spotify_id = spotify_id
        self.name = name
        self.children = []
        self.depth = depth
        self.downloaded = False
        self.deezer_id = None

class ArtistProcessor(threading.Thread):
    def __init__(self, spotify_id, deezer_id, output_dir, album_types, max_retries, thread_id, node, output_callback, progress_callback, artist_finished_callback):
        super().__init__()
        self.spotify_id = spotify_id
        self.deezer_id = deezer_id
        self.output_dir = output_dir
        self.album_types = album_types
        self.max_retries = max_retries
        self.thread_id = thread_id
        self.node = node
        self.sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
        self.dz = API(requests.Session(), {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        self.is_running = True
        self.output_callback = output_callback
        self.progress_callback = progress_callback
        self.artist_finished_callback = artist_finished_callback

    def run(self):
        try:
            spotify_artist = self.sp.artist(self.spotify_id)
            deezer_artist = self.dz.get_artist(self.deezer_id)
            
            self.output_callback(f"Thread {self.thread_id}: Processing artist: {spotify_artist['name']}")
            success = self.download_artist(deezer_artist)
            self.artist_finished_callback(f"{spotify_artist['name']}|{self.spotify_id}|{self.deezer_id}", success)
        except Exception as e:
            self.output_callback(f"Thread {self.thread_id}: Error processing artist: {str(e)}")
            logging.error(f"Error processing artist {self.spotify_id}: {str(e)}")
            self.artist_finished_callback(f"Unknown|{self.spotify_id}|{self.deezer_id}", False)

    def download_artist(self, deezer_artist):
        try:
            artist_dir = os.path.join(self.output_dir, self.node.name)
            os.makedirs(artist_dir, exist_ok=True)
            
            deemix_cmd = f'deemix -p "{artist_dir}" "https://www.deezer.com/artist/{deezer_artist["id"]}"'
            
            # Use a batch file to run deemix and close the CMD window when finished
            batch_file_path = os.path.join(artist_dir, f"download_{deezer_artist['id']}.bat")
            with open(batch_file_path, 'w') as batch_file:
                batch_file.write(f"@echo off\n")
                batch_file.write(f"{deemix_cmd}\n")
                batch_file.write("echo Download completed!\n")
                batch_file.write("exit\n")
            
            process = subprocess.Popen(f'start /wait cmd /c "{batch_file_path}"', shell=True)
            
            process.wait()
            
            # Remove the temporary batch file
            os.remove(batch_file_path)
            
            self.output_callback(f"Thread {self.thread_id}: Successfully downloaded artist: {deezer_artist['name']}")
            self.progress_callback(100, deezer_artist['name'])
            return True
        except Exception as e:
            self.output_callback(f"Thread {self.thread_id}: Error downloading artist {deezer_artist['name']}: {str(e)}")
            logging.error(f"Error downloading artist {deezer_artist['name']}: {str(e)}")
            return False

    def stop(self):
        self.is_running = False

class SpotifyDeezerWildChainDownloader(threading.Thread):
    def __init__(self, spotify_link, output_dir, max_artists, album_types, max_retries, max_concurrent, wild_branching, wildly_different, max_depth, related_limit, output_callback, progress_callback, artist_finished_callback, tree_updated_callback, finished_callback):
        super().__init__()
        self.spotify_link = spotify_link
        self.output_dir = output_dir
        self.max_artists = max_artists
        self.album_types = album_types
        self.max_retries = max_retries
        self.max_concurrent = max_concurrent
        self.wild_branching = wild_branching
        self.wildly_different = wildly_different
        self.max_depth = max_depth
        self.related_limit = related_limit
        self.is_running = True
        self.sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
        self.dz = API(requests.Session(), {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        self.processed_artists = set()
        self.artist_queue = queue.Queue()
        self.lock = threading.Lock()
        self.root_node = None
        self.current_depth = 0
        self.output_callback = output_callback
        self.progress_callback = progress_callback
        self.artist_finished_callback = artist_finished_callback
        self.tree_updated_callback = tree_updated_callback
        self.finished_callback = finished_callback
        self.processed_genres = set()
        self.batch_files = []

    def run(self):
        if not is_valid_url(self.spotify_link, ['open.spotify.com']):
            self.output_callback("Invalid Spotify URL. Please provide a valid Spotify artist link.")
            self.finished_callback()
            return

        try:
            artist_id = re.search(r'artist/(\w+)', self.spotify_link).group(1)
            spotify_artist = self.sp.artist(artist_id)
            self.root_node = ArtistNode(artist_id, spotify_artist['name'])
            self.tree_updated_callback(self.root_node)
            self.artist_queue.put(self.root_node)

            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                while not self.artist_queue.empty() and len(self.processed_artists) < self.max_artists and self.is_running:
                    futures = []
                    for _ in range(min(self.max_concurrent, self.artist_queue.qsize())):
                        if not self.artist_queue.empty():
                            node = self.artist_queue.get()
                            futures.append(executor.submit(self.process_artist, node))
                    
                    for future in as_completed(futures):
                        future.result()

            self.finished_callback()
        except Exception as e:
            self.output_callback(f"An error occurred: {str(e)}")
            logging.error(f"Error in main download process: {str(e)}")
            self.finished_callback()
        finally:
            self.cleanup_batch_files()

    def process_artist(self, node):
        with self.lock:
            if node.spotify_id in self.processed_artists or len(self.processed_artists) >= self.max_artists:
                return
            self.processed_artists.add(node.spotify_id)

        try:
            spotify_artist = self.sp.artist(node.spotify_id)
            deezer_artist = self.find_deezer_artist(spotify_artist['name'])
            
            if deezer_artist:
                node.deezer_id = deezer_artist['id']
                success = self.download_artist(deezer_artist, node)
                self.artist_finished_callback(f"{spotify_artist['name']}|{node.spotify_id}|{node.deezer_id}", success)

                if len(self.processed_artists) < self.max_artists and node.depth < self.max_depth:
                    related_artists = self.sp.artist_related_artists(node.spotify_id)['artists']
                    if self.wild_branching:
                        random.shuffle(related_artists)
                    if self.wildly_different:
                        related_artists = self.get_wildly_different_artists(spotify_artist, related_artists)
                    for related_artist in related_artists[:self.related_limit]:
                        child_node = ArtistNode(related_artist['id'], related_artist['name'], node.depth + 1)
                        node.children.append(child_node)
                        self.artist_queue.put(child_node)
                    self.tree_updated_callback(self.root_node)
            else:
                self.output_callback(f"Could not find Deezer artist for: {node.name}")
        except Exception as e:
            self.output_callback(f"Error processing artist {node.name}: {str(e)}")
            logging.error(f"Error processing artist {node.name}: {str(e)}")

    def download_artist(self, deezer_artist, node):
        try:
            artist_dir = os.path.join(self.output_dir, node.name)
            os.makedirs(artist_dir, exist_ok=True)
            
            deemix_cmd = f'deemix -p "{artist_dir}" "https://www.deezer.com/artist/{deezer_artist["id"]}"'
            
            batch_file_path = os.path.join(artist_dir, f"download_{deezer_artist['id']}.bat")
            with open(batch_file_path, 'w') as batch_file:
                batch_file.write(f"@echo off\n")
                batch_file.write(f"{deemix_cmd}\n")
                batch_file.write("echo Download completed!\n")
                batch_file.write("exit\n")
            
            self.batch_files.append(batch_file_path)
            
            process = subprocess.Popen(f'start /wait cmd /c "{batch_file_path}"', shell=True)
            
            process.wait()
            
            self.output_callback(f"Successfully downloaded artist: {deezer_artist['name']}")
            self.progress_callback(100, deezer_artist['name'])
            return True
        except Exception as e:
            self.output_callback(f"Error downloading artist {deezer_artist['name']}: {str(e)}")
            logging.error(f"Error downloading artist {deezer_artist['name']}: {str(e)}")
            return False

    def cleanup_batch_files(self):
        for batch_file in self.batch_files:
            try:
                os.remove(batch_file)
            except Exception as e:
                logging.error(f"Error removing batch file {batch_file}: {str(e)}")

    def get_wildly_different_artists(self, source_artist, related_artists):
        source_genres = set(source_artist['genres'])
        self.processed_genres.update(source_genres)
        
        wildly_different = []
        for artist in related_artists:
            try:
                artist_full = self.sp.artist(artist['id'])
                artist_genres = set(artist_full['genres'])
                
                # Check if the artist has any genres we haven't processed yet
                new_genres = artist_genres - self.processed_genres
                
                if new_genres:
                    wildly_different.append(artist)
                    self.processed_genres.update(new_genres)
                
                if len(wildly_different) >= self.related_limit:
                    break
            except Exception as e:
                self.output_callback(f"Error processing related artist {artist['name']}: {str(e)}")
                logging.error(f"Error processing related artist {artist['name']}: {str(e)}")
        
        # If we don't have enough wildly different artists, add some random ones
        if len(wildly_different) < self.related_limit:
            remaining = [a for a in related_artists if a not in wildly_different]
            wildly_different.extend(random.sample(remaining, min(len(remaining), self.related_limit - len(wildly_different))))
        
        return wildly_different

    def find_deezer_artist(self, artist_name):
        try:
            result = self.dz.search_artist(artist_name, limit=1)
            if result['data']:
                return result['data'][0]
        except Exception as e:
            self.output_callback(f"Error finding Deezer artist: {str(e)}")
            logging.error(f"Error finding Deezer artist {artist_name}: {str(e)}")
        return None

    def stop(self):
        self.is_running = False
        self.cleanup_batch_files()

class SpotifyDeemixGUI:
    def __init__(self, master):
        self.master = master
        self.master.title('Deez Nuts - Mass Downloader')
        self.master.geometry('1000x800')

        self.notebook = ttk.Notebook(self.master)
        self.chain_tab = ttk.Frame(self.notebook)
        self.settings_tab = ttk.Frame(self.notebook)
        self.history_tab = ttk.Frame(self.notebook)
        self.log_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.chain_tab, text="Chain Download")
        self.notebook.add(self.settings_tab, text="Settings")
        self.notebook.add(self.history_tab, text="History")
        self.notebook.add(self.log_tab, text="Log")
        self.notebook.pack(expand=1, fill="both")

        self.setup_chain_tab()
        self.setup_settings_tab()
        self.setup_history_tab()
        self.setup_log_tab()

        self.settings = {}
        self.load_settings()

        self.download_history = []
        self.load_history()

    def setup_chain_tab(self):
        frame = ttk.Frame(self.chain_tab)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="Spotify Artist URL:").grid(row=0, column=0, sticky=tk.W)
        self.url_input = ttk.Entry(frame, width=50)
        self.url_input.grid(row=0, column=1, columnspan=3, sticky=tk.W+tk.E)

        ttk.Label(frame, text="Max artists:").grid(row=1, column=0, sticky=tk.W)
        self.max_artists_input = ttk.Spinbox(frame, from_=1, to=1000, width=5)
        self.max_artists_input.set(50)
        self.max_artists_input.grid(row=1, column=1, sticky=tk.W)

        ttk.Label(frame, text="Max depth:").grid(row=1, column=2, sticky=tk.W)
        self.max_depth_input = ttk.Spinbox(frame, from_=1, to=10, width=5)
        self.max_depth_input.set(3)
        self.max_depth_input.grid(row=1, column=3, sticky=tk.W)

        ttk.Label(frame, text="Max concurrent:").grid(row=2, column=0, sticky=tk.W)
        self.max_concurrent_input = ttk.Spinbox(frame, from_=1, to=20, width=5)
        self.max_concurrent_input.set(5)
        self.max_concurrent_input.grid(row=2, column=1, sticky=tk.W)

        ttk.Label(frame, text="Related artists limit:").grid(row=2, column=2, sticky=tk.W)
        self.related_limit_input = ttk.Spinbox(frame, from_=1, to=20, width=5)
        self.related_limit_input.set(5)
        self.related_limit_input.grid(row=2, column=3, sticky=tk.W)

        ttk.Label(frame, text="Album types to download:").grid(row=3, column=0, sticky=tk.W)
        self.album_types_input = tk.Listbox(frame, selectmode=tk.MULTIPLE, height=3)
        for item in ["album", "single", "compilation"]:
            self.album_types_input.insert(tk.END, item)
            self.album_types_input.selection_set(tk.END)
        self.album_types_input.grid(row=3, column=1, columnspan=3, sticky=tk.W+tk.E)

        self.wild_branching_var = tk.BooleanVar()
        self.wild_branching_checkbox = ttk.Checkbutton(frame, text="Enable wild branching", variable=self.wild_branching_var)
        self.wild_branching_checkbox.grid(row=4, column=0, columnspan=2, sticky=tk.W)

        self.wildly_different_var = tk.BooleanVar()
        self.wildly_different_checkbox = ttk.Checkbutton(frame, text="Enable wildly different artists", variable=self.wildly_different_var)
        self.wildly_different_checkbox.grid(row=4, column=2, columnspan=2, sticky=tk.W)

        self.start_button = ttk.Button(frame, text='Start Chain Download', command=self.start_chain_download)
        self.start_button.grid(row=5, column=0, columnspan=2, sticky=tk.W)

        self.stop_button = ttk.Button(frame, text='Stop', command=self.stop_chain_download, state=tk.DISABLED)
        self.stop_button.grid(row=5, column=2, columnspan=2, sticky=tk.W)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=6, column=0, columnspan=4, sticky=tk.W+tk.E)

        self.output_display = tk.Text(frame, wrap=tk.WORD, width=50, height=10)
        self.output_display.grid(row=7, column=0, columnspan=2, sticky=tk.W+tk.E+tk.N+tk.S)

        self.artist_tree = ttk.Treeview(frame, columns=('Status',), height=10)
        self.artist_tree.heading('#0', text='Artist')
        self.artist_tree.heading('Status', text='Status')
        self.artist_tree.column('Status', width=100)
        self.artist_tree.grid(row=7, column=2, columnspan=2, sticky=tk.W+tk.E+tk.N+tk.S)

        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)
        frame.rowconfigure(7, weight=1)

    def setup_settings_tab(self):
        frame = ttk.Frame(self.settings_tab)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="Spotify API Credentials:").grid(row=0, column=0, sticky=tk.W)
        ttk.Label(frame, text="Client ID:").grid(row=1, column=0, sticky=tk.W)
        self.client_id_input = ttk.Entry(frame, width=50)
        self.client_id_input.grid(row=1, column=1, sticky=tk.W)

        ttk.Label(frame, text="Client Secret:").grid(row=2, column=0, sticky=tk.W)
        self.client_secret_input = ttk.Entry(frame, width=50, show="*")
        self.client_secret_input.grid(row=2, column=1, sticky=tk.W)

        ttk.Label(frame, text="Output Directory:").grid(row=3, column=0, sticky=tk.W)
        self.output_dir_input = ttk.Entry(frame, width=50)
        self.output_dir_input.grid(row=3, column=1, sticky=tk.W)
        self.output_dir_button = ttk.Button(frame, text="Browse", command=self.browse_output_dir)
        self.output_dir_button.grid(row=3, column=2, sticky=tk.W)

        ttk.Label(frame, text="Max Retries:").grid(row=4, column=0, sticky=tk.W)
        self.max_retries_input = ttk.Spinbox(frame, from_=0, to=10, width=5)
        self.max_retries_input.set(3)
        self.max_retries_input.grid(row=4, column=1, sticky=tk.W)

        self.save_settings_button = ttk.Button(frame, text="Save Settings", command=self.save_settings)
        self.save_settings_button.grid(row=5, column=0, columnspan=2, sticky=tk.W)

    def setup_history_tab(self):
        frame = ttk.Frame(self.history_tab)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.history_list = tk.Listbox(frame, width=80, height=20)
        self.history_list.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.clear_history_button = ttk.Button(frame, text="Clear History", command=self.clear_history)
        self.clear_history_button.pack(side=tk.BOTTOM)

    def setup_log_tab(self):
        frame = ttk.Frame(self.log_tab)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.log_display = tk.Text(frame, wrap=tk.WORD, width=80, height=20)
        self.log_display.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.clear_log_button = ttk.Button(frame, text="Clear Log", command=self.clear_log)
        self.clear_log_button.pack(side=tk.BOTTOM)

    def load_settings(self):
        try:
            with open('settings.json', 'r') as f:
                self.settings = json.load(f)
        except FileNotFoundError:
            self.settings = {}

        self.client_id_input.insert(0, self.settings.get('client_id', ''))
        self.client_secret_input.insert(0, self.settings.get('client_secret', ''))
        self.output_dir_input.insert(0, self.settings.get('output_dir', ''))
        self.max_retries_input.set(self.settings.get('max_retries', 3))

        os.environ['SPOTIPY_CLIENT_ID'] = self.client_id_input.get()
        os.environ['SPOTIPY_CLIENT_SECRET'] = self.client_secret_input.get()

    def save_settings(self):
        self.settings = {
            'client_id': self.client_id_input.get(),
            'client_secret': self.client_secret_input.get(),
            'output_dir': self.output_dir_input.get(),
            'max_retries': int(self.max_retries_input.get()),
        }
        with open('settings.json', 'w') as f:
            json.dump(self.settings, f)

        os.environ['SPOTIPY_CLIENT_ID'] = self.settings['client_id']
        os.environ['SPOTIPY_CLIENT_SECRET'] = self.settings['client_secret']

        messagebox.showinfo("Settings Saved", "Your settings have been saved and applied.")

    def browse_output_dir(self):
        dir_path = filedialog.askdirectory()
        if dir_path:
            self.output_dir_input.delete(0, tk.END)
            self.output_dir_input.insert(0, dir_path)

    def start_chain_download(self):
        spotify_link = self.url_input.get()
        if not is_valid_url(spotify_link, ['open.spotify.com']):
            messagebox.showerror("Invalid Input", "Please enter a valid Spotify artist link.")
            return

        selected_album_types = [self.album_types_input.get(i) for i in self.album_types_input.curselection()]
        if not selected_album_types:
            messagebox.showerror("Invalid Selection", "Please select at least one album type.")
            return

        self.downloader = SpotifyDeezerWildChainDownloader(
            spotify_link,
            self.output_dir_input.get(),
            int(self.max_artists_input.get()),
            selected_album_types,
            int(self.max_retries_input.get()),
            int(self.max_concurrent_input.get()),
            self.wild_branching_var.get(),
            self.wildly_different_var.get(),
            int(self.max_depth_input.get()),
            int(self.related_limit_input.get()),
            self.update_output,
            self.update_progress,
            self.add_to_history,
            self.update_artist_tree,
            self.download_finished
        )
        self.downloader.start()

        self.start_button['state'] = tk.DISABLED
        self.stop_button['state'] = tk.NORMAL
        self.progress_var.set(0)

    def stop_chain_download(self):
        if hasattr(self, 'downloader'):
            self.downloader.stop()
        self.stop_button['state'] = tk.DISABLED
        self.update_output("Stopping download after current artists finish...")

    def update_output(self, text):
        self.output_display.insert(tk.END, text + '\n')
        self.output_display.see(tk.END)
        self.log_display.insert(tk.END, text + '\n')
        self.log_display.see(tk.END)

    def update_progress(self, value, artist_name):
        self.progress_var.set(value)
        self.progress_bar['value'] = value

    def add_to_history(self, artist_name, success):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        status = "Successfully downloaded" if success else "Failed to download"
        history_item = f"{timestamp} - {status} {artist_name}"
        self.download_history.append(history_item)
        self.history_list.insert(tk.END, history_item)
        self.save_history()

    def save_history(self):
        with open('download_history.json', 'w') as f:
            json.dump(self.download_history, f)

    def load_history(self):
        try:
            with open('download_history.json', 'r') as f:
                self.download_history = json.load(f)
            for item in self.download_history:
                self.history_list.insert(tk.END, item)
        except FileNotFoundError:
            self.download_history = []

    def clear_history(self):
        if messagebox.askyesno('Clear History', 'Are you sure you want to clear the download history?'):
            self.download_history.clear()
            self.history_list.delete(0, tk.END)
            self.save_history()

    def clear_log(self):
        if messagebox.askyesno('Clear Log', 'Are you sure you want to clear the log?'):
            self.log_display.delete('1.0', tk.END)

    def download_finished(self):
        self.start_button['state'] = tk.NORMAL
        self.stop_button['state'] = tk.DISABLED
        self.update_output("Chain download completed.")
        self.progress_var.set(100)
        messagebox.showinfo("Download Complete", "The chain download has finished.")

    def update_artist_tree(self, root_node):
        self.artist_tree.delete(*self.artist_tree.get_children())
        self._add_node(root_node, '')

    def _add_node(self, node, parent):
        tree_id = self.artist_tree.insert(parent, 'end', text=node.name, values=('Pending',))
        for child in node.children:
            self._add_node(child, tree_id)

    def update_artist_status(self, artist_name, success):
        for item in self.artist_tree.get_children():
            if self._update_status(item, artist_name, success):
                break

    def _update_status(self, item, artist_name, success):
        if self.artist_tree.item(item, 'text') == artist_name:
            self.artist_tree.set(item, 'Status', 'Downloaded' if success else 'Failed')
            return True
        for child in self.artist_tree.get_children(item):
            if self._update_status(child, artist_name, success):
                return True
        return False

if __name__ == '__main__':
    root = tk.Tk()
    app = SpotifyDeemixGUI(root)
    root.mainloop()
