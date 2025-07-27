#!/usr/bin/env python3
"""
Duplicate Photo Manager - Eenvoudige Gecombineerde App
"""

import sys
import os
import sqlite3
import imagehash
from PIL import Image, ExifTags
import pillow_heif
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from collections import defaultdict
import subprocess
import platform

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QFileDialog, QMessageBox, QProgressBar,
    QLineEdit, QTabWidget, QScrollArea, QFrame, QGridLayout,
    QSizePolicy, QSpacerItem, QTextEdit, QGroupBox
)
from PySide6.QtGui import QPixmap, QFont
from PySide6.QtCore import Qt, Signal, QThread

# Register HEIF support
pillow_heif.register_heif_opener()

# Image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.heif', '.bmp', '.gif', '.webp', '.tiff', '.tif'}

class ScanThread(QThread):
    """Background thread voor het scannen"""
    
    progress_text = Signal(str)
    progress_value = Signal(int)
    finished_scan = Signal(bool, str)  # success, message
    
    def __init__(self, source_folder, db_path):
        super().__init__()
        self.source_folder = source_folder
        self.db_path = db_path
        self.should_stop = False
    
    def stop(self):
        self.should_stop = True
    
    def run(self):
        try:
            self.scan_for_duplicates()
        except Exception as e:
            self.finished_scan.emit(False, f"Fout tijdens scannen: {str(e)}")
    
    def scan_for_duplicates(self):
        """Scan voor duplicaten (vereenvoudigde versie)"""
        
        # Setup database
        self.progress_text.emit("Database voorbereiden...")
        self.setup_database()
        
        # Scan bestanden
        self.progress_text.emit("Bestanden zoeken...")
        image_files = self.find_image_files()
        
        if not image_files:
            self.finished_scan.emit(False, "Geen afbeeldingen gevonden!")
            return
        
        self.progress_text.emit(f"Verwerken van {len(image_files)} afbeeldingen...")
        
        # Process images
        processed = 0
        hash_to_images = defaultdict(list)
        
        for filepath in image_files:
            if self.should_stop:
                return
            
            try:
                img_hash = self.get_image_hash(filepath)
                if img_hash:
                    metadata = self.get_metadata(filepath)
                    
                    image_data = {
                        "path": filepath,
                        "hash": str(img_hash),
                        "filename": os.path.basename(filepath),
                        **metadata
                    }
                    
                    self.save_image_to_db(image_data)
                    hash_to_images[str(img_hash)].append(image_data)
                
                processed += 1
                progress = int((processed / len(image_files)) * 80)  # 80% voor processing
                self.progress_value.emit(progress)
                
            except Exception as e:
                print(f"Fout bij {filepath}: {e}")
                continue
        
        # Find duplicates
        self.progress_text.emit("Duplicaten zoeken...")
        duplicate_count = 0
        
        for img_hash, images in hash_to_images.items():
            if len(images) > 1:
                # Bepaal origineel
                images.sort(key=lambda x: (x['width'] * x['height'], x['date_taken'] or ''), reverse=True)
                
                # Maak groep
                group_id = self.create_duplicate_group(img_hash, images)
                
                # Update images - GEEN origineel meer, alles is verwijderbaar
                for img in images:
                    self.update_image_group(img['path'], group_id, False)  # Alles op False
                
                duplicate_count += 1
        
        self.progress_value.emit(100)
        
        if duplicate_count == 0:
            self.finished_scan.emit(True, "Geen duplicaten gevonden! Alle afbeeldingen zijn uniek.")
        else:
            total_duplicates = sum(len(images) - 1 for images in hash_to_images.values() if len(images) > 1)
            self.finished_scan.emit(True, f"Scan voltooid!\n{duplicate_count} groepen met {total_duplicates} duplicaten gevonden.")
    
    def setup_database(self):
        """Setup database tabellen"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                DROP TABLE IF EXISTS images;
                DROP TABLE IF EXISTS duplicate_groups;
                
                CREATE TABLE images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT UNIQUE NOT NULL,
                    filename TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    resolution TEXT,
                    width INTEGER,
                    height INTEGER,
                    file_size INTEGER,
                    date_taken TEXT,
                    date_modified TEXT,
                    is_original BOOLEAN DEFAULT FALSE,
                    group_id INTEGER,
                    is_deleted BOOLEAN DEFAULT FALSE
                );
                
                CREATE TABLE duplicate_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash TEXT NOT NULL,
                    image_count INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_hash ON images(hash);
                CREATE INDEX idx_group_id ON images(group_id);
                CREATE INDEX idx_path ON images(path);
            """)
    
    def find_image_files(self):
        """Vind alle afbeeldingen"""
        folder_path = Path(self.source_folder)
        image_files = []
        
        for ext in IMAGE_EXTENSIONS:
            pattern = f"**/*{ext}"
            files = list(folder_path.glob(pattern))
            image_files.extend([str(f) for f in files if f.is_file()])
            
            pattern = f"**/*{ext.upper()}"
            files = list(folder_path.glob(pattern))
            image_files.extend([str(f) for f in files if f.is_file()])
        
        return list(set(image_files))
    
    def get_image_hash(self, path):
        """Bereken image hash"""
        try:
            with Image.open(path) as img:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                return imagehash.phash(img, hash_size=16)
        except:
            return None
    
    def get_metadata(self, path):
        """Verkrijg metadata"""
        try:
            stat = os.stat(path)
            size = stat.st_size
            date_modified = datetime.fromtimestamp(stat.st_mtime).isoformat()
            
            with Image.open(path) as img:
                width, height = img.size
                resolution = f"{width}x{height}"
                
                # EXIF datum
                date_taken = None
                try:
                    if hasattr(img, '_getexif') and img._getexif():
                        exif = img._getexif()
                        for tag, value in exif.items():
                            if ExifTags.TAGS.get(tag) == 'DateTime':
                                date_taken = datetime.strptime(value, '%Y:%m:%d %H:%M:%S').isoformat()
                                break
                except:
                    pass
                
                return {
                    "resolution": resolution,
                    "width": width,
                    "height": height,
                    "file_size": size,
                    "date_taken": date_taken,
                    "date_modified": date_modified
                }
        except:
            return {
                "resolution": "unknown",
                "width": 0,
                "height": 0,
                "file_size": 0,
                "date_taken": None,
                "date_modified": None
            }
    
    def save_image_to_db(self, image_data):
        """Sla afbeelding op in database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO images 
                (path, filename, hash, resolution, width, height, file_size, date_taken, date_modified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                image_data["path"], image_data["filename"], image_data["hash"],
                image_data["resolution"], image_data["width"], image_data["height"],
                image_data["file_size"], image_data["date_taken"], image_data["date_modified"]
            ))
    
    def create_duplicate_group(self, img_hash, images):
        """Maak duplicate groep"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO duplicate_groups (hash, image_count) VALUES (?, ?)
            """, (img_hash, len(images)))
            return cursor.lastrowid
    
    def update_image_group(self, path, group_id, is_original):
        """Update image met groep info"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE images SET group_id = ?, is_original = ? WHERE path = ?
            """, (group_id, is_original, path))

class ImageCard(QFrame):
    """Eenvoudige image card - alles is verwijderbaar"""
    
    deleteRequested = Signal(str)
    
    def __init__(self, image_data):
        super().__init__()
        self.image_data = image_data
        self.setup_ui()
    
    def setup_ui(self):
        self.setFixedSize(200, 250)
        
        # Alle cards hebben dezelfde styling - geen origineel onderscheid
        self.setStyleSheet("QFrame { border: 2px solid #444; border-radius: 8px; background: #2a2a2a; }")
        
        layout = QVBoxLayout(self)
        layout.setSpacing(5)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Geen origineel badge meer
        
        # Afbeelding
        img_label = QLabel()
        img_label.setFixedHeight(120)
        img_label.setStyleSheet("border: 1px solid #333; border-radius: 4px; background: #1a1a1a;")
        
        try:
            pixmap = QPixmap(self.image_data["path"])
            if not pixmap.isNull():
                pixmap = pixmap.scaled(180, 110, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                img_label.setPixmap(pixmap)
                img_label.setAlignment(Qt.AlignCenter)
                img_label.setCursor(Qt.PointingHandCursor)
                img_label.mousePressEvent = lambda e: self.open_image()
        except:
            img_label.setText("Kan niet laden")
            img_label.setAlignment(Qt.AlignCenter)
            img_label.setStyleSheet(img_label.styleSheet() + "color: #ff6b6b;")
        
        layout.addWidget(img_label)
        
        # Info
        filename = QLabel(os.path.basename(self.image_data["path"])[:20] + "...")
        filename.setStyleSheet("color: white; font-size: 10px;")
        layout.addWidget(filename)
        
        size_kb = round(self.image_data.get("file_size", 0) / 1024)
        size_text = f"{size_kb} KB" if size_kb < 1024 else f"{round(size_kb/1024, 1)} MB"
        size_label = QLabel(f"{self.image_data.get('resolution', 'unknown')} • {size_text}")
        size_label.setStyleSheet("color: #ccc; font-size: 9px;")
        layout.addWidget(size_label)
        
        # Iedereen krijgt een delete knop
        delete_btn = QPushButton("Verwijderen")
        delete_btn.setFixedHeight(24)
        delete_btn.setStyleSheet("""
            QPushButton { 
                background: #cc3333; color: white; border: none; 
                border-radius: 4px; font-size: 9px; 
            }
            QPushButton:hover { background: #dd4444; }
        """)
        delete_btn.clicked.connect(lambda: self.deleteRequested.emit(self.image_data["path"]))
        layout.addWidget(delete_btn)
    
    def open_image(self):
        """Open afbeelding in standaard app"""
        try:
            if platform.system() == "Darwin":
                subprocess.run(["open", self.image_data["path"]])
            elif platform.system() == "Windows":
                os.startfile(self.image_data["path"])
            else:
                subprocess.run(["xdg-open", self.image_data["path"]])
        except:
            QMessageBox.warning(self, "Fout", "Kan afbeelding niet openen")

class DuplicatePhotoManager(QMainWindow):
    """Hoofdapplicatie"""
    
    def __init__(self):
        super().__init__()
        self.scan_thread = None
        self.duplicates = []
        self.current_group = 0
        self.db_path = "duplicates.db"
        
        self.setup_ui()
        self.load_existing_results()
    
    def setup_ui(self):
        self.setWindowTitle("Duplicate Photo Manager")
        self.setMinimumSize(1000, 700)
        self.resize(1200, 800)
        
        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        
        # Main layout
        layout = QVBoxLayout(central)
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Tab widget
        tabs = QTabWidget()
        
        # Tab 1: Nieuwe scan
        scan_tab = self.create_scan_tab()
        tabs.addTab(scan_tab, "Nieuwe Scan")
        
        # Tab 2: Resultaten bekijken
        results_tab = self.create_results_tab()
        tabs.addTab(results_tab, "Resultaten Bekijken")
        
        layout.addWidget(tabs)
        
        # Styling
        self.setStyleSheet("""
            QMainWindow { background: #1e1e1e; }
            QWidget { background: #1e1e1e; color: white; font-family: Arial, sans-serif; }
            QTabWidget::pane { border: 1px solid #444; }
            QTabBar::tab { background: #2a2a2a; padding: 8px 16px; margin: 2px; border-radius: 4px; }
            QTabBar::tab:selected { background: #4a9eff; }
            QPushButton { 
                background: #3498db; color: white; border: none; 
                border-radius: 6px; padding: 10px; font-weight: bold; 
            }
            QPushButton:hover { background: #2980b9; }
            QPushButton:disabled { background: #555; }
            QLineEdit { 
                background: #2a2a2a; border: 2px solid #444; 
                border-radius: 4px; padding: 8px; color: white; 
            }
            QLineEdit:focus { border-color: #4a9eff; }
            QLabel { color: white; }
            QProgressBar { 
                border: 2px solid #444; border-radius: 4px; 
                text-align: center; background: #2a2a2a; 
            }
            QProgressBar::chunk { background: #4a9eff; border-radius: 2px; }
        """)
    
    def create_scan_tab(self):
        """Maak scan tab"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        layout.setContentsMargins(40, 40, 40, 40)
        
        # Title
        title = QLabel("Nieuwe Scan Starten")
        title.setStyleSheet("font-size: 24px; font-weight: bold; color: #4a9eff;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # Folder selection
        folder_group = QGroupBox("Map Selecteren")
        folder_group.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #444; border-radius: 8px; margin: 10px; padding-top: 10px; }")
        folder_layout = QVBoxLayout(folder_group)
        
        self.folder_input = QLineEdit()
        self.folder_input.setPlaceholderText("Selecteer een map om te scannen...")
        self.folder_input.setText("(example: /Pictures)")
        folder_layout.addWidget(self.folder_input)
        
        browse_btn = QPushButton("Bladeren...")
        browse_btn.clicked.connect(self.browse_folder)
        folder_layout.addWidget(browse_btn)
        
        layout.addWidget(folder_group)
        
        # Progress
        self.progress_label = QLabel("Klaar om te scannen")
        self.progress_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.progress_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.scan_btn = QPushButton("🔍 Start Scan")
        self.scan_btn.clicked.connect(self.start_scan)
        button_layout.addWidget(self.scan_btn)
        
        self.stop_btn = QPushButton("⏹ Stop")
        self.stop_btn.clicked.connect(self.stop_scan)
        self.stop_btn.setVisible(False)
        self.stop_btn.setStyleSheet("QPushButton { background: #e74c3c; } QPushButton:hover { background: #c0392b; }")
        button_layout.addWidget(self.stop_btn)
        
        layout.addLayout(button_layout)
        layout.addStretch()
        
        return widget
    
    def create_results_tab(self):
        """Maak resultaten tab"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header
        header = QHBoxLayout()
        
        self.results_label = QLabel("Geen resultaten geladen")
        self.results_label.setStyleSheet("font-size: 16px; font-weight: bold;")
        header.addWidget(self.results_label)
        
        header.addStretch()
        
        refresh_btn = QPushButton("🔄 Vernieuwen")
        refresh_btn.clicked.connect(self.load_existing_results)
        header.addWidget(refresh_btn)
        
        layout.addLayout(header)
        
        # Navigation
        nav_layout = QHBoxLayout()
        
        self.prev_btn = QPushButton("← Vorige")
        self.prev_btn.clicked.connect(self.prev_group)
        self.prev_btn.setEnabled(False)
        nav_layout.addWidget(self.prev_btn)
        
        self.group_label = QLabel("Groep 0 van 0")
        self.group_label.setAlignment(Qt.AlignCenter)
        nav_layout.addWidget(self.group_label)
        
        self.next_btn = QPushButton("Volgende →")
        self.next_btn.clicked.connect(self.next_group)
        self.next_btn.setEnabled(False)
        nav_layout.addWidget(self.next_btn)
        
        layout.addLayout(nav_layout)
        
        # Images
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("QScrollArea { border: 1px solid #444; border-radius: 8px; background: #2a2a2a; }")
        
        self.scroll_content = QWidget()
        self.images_layout = QGridLayout(self.scroll_content)
        self.images_layout.setSpacing(10)
        
        self.scroll_area.setWidget(self.scroll_content)
        layout.addWidget(self.scroll_area)
        
        return widget
    
    def browse_folder(self):
        """Open folder dialog"""
        folder = QFileDialog.getExistingDirectory(
            self, 
            "Selecteer map om te scannen",
            self.folder_input.text() or os.path.expanduser("~/Pictures")
        )
        if folder:
            self.folder_input.setText(folder)
    
    def start_scan(self):
        """Start scan process"""
        folder = self.folder_input.text().strip()
        
        if not folder:
            QMessageBox.warning(self, "Fout", "Selecteer eerst een map!")
            return
        
        if not os.path.exists(folder):
            QMessageBox.warning(self, "Fout", "Map bestaat niet!")
            return
        
        # UI updates
        self.scan_btn.setVisible(False)
        self.stop_btn.setVisible(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Scannen gestart...")
        
        # Start scan thread
        self.scan_thread = ScanThread(folder, self.db_path)
        self.scan_thread.progress_text.connect(self.progress_label.setText)
        self.scan_thread.progress_value.connect(self.progress_bar.setValue)
        self.scan_thread.finished_scan.connect(self.scan_finished)
        self.scan_thread.start()
    
    def stop_scan(self):
        """Stop scan"""
        if self.scan_thread:
            self.scan_thread.stop()
            self.scan_thread.quit()
            self.scan_thread.wait()
        
        self.reset_scan_ui()
        self.progress_label.setText("Scan gestopt")
    
    def scan_finished(self, success, message):
        """Scan voltooid"""
        self.reset_scan_ui()
        
        if success:
            self.progress_label.setText("Scan voltooid!")
            QMessageBox.information(self, "Scan Voltooid", message)
            self.load_existing_results()
        else:
            self.progress_label.setText("Scan gefaald")
            QMessageBox.critical(self, "Scan Fout", message)
    
    def reset_scan_ui(self):
        """Reset scan UI"""
        self.scan_btn.setVisible(True)
        self.stop_btn.setVisible(False)
        self.progress_bar.setVisible(False)
        
        if self.scan_thread:
            self.scan_thread.quit()
            self.scan_thread.wait()
            self.scan_thread = None
    
    def load_existing_results(self):
        """Laad bestaande resultaten"""
        if not os.path.exists(self.db_path):
            self.results_label.setText("Geen database gevonden - voer eerst een scan uit")
            self.update_navigation()
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Haal groepen op
                groups = conn.execute("""
                    SELECT dg.id, dg.hash, COUNT(i.id) as image_count
                    FROM duplicate_groups dg
                    JOIN images i ON dg.id = i.group_id 
                    WHERE i.is_deleted = FALSE
                    GROUP BY dg.id, dg.hash
                    HAVING COUNT(i.id) > 1
                    ORDER BY COUNT(i.id) DESC
                """).fetchall()
                
                self.duplicates = []
                for group in groups:
                    # Haal afbeeldingen op
                    images = conn.execute("""
                        SELECT * FROM images 
                        WHERE group_id = ? AND is_deleted = FALSE
                        ORDER BY is_original DESC, width * height DESC
                    """, (group["id"],)).fetchall()
                    
                    if len(images) > 1:
                        self.duplicates.append([dict(img) for img in images])
                
                if self.duplicates:
                    self.results_label.setText(f"{len(self.duplicates)} duplicate groepen gevonden")
                    self.current_group = 0
                else:
                    self.results_label.setText("Geen duplicaten gevonden")
                    self.current_group = 0
                
                self.update_navigation()
                self.show_current_group()
                
        except Exception as e:
            self.results_label.setText(f"Fout bij laden: {str(e)}")
            self.update_navigation()
    
    def prev_group(self):
        """Vorige groep"""
        if self.current_group > 0:
            self.current_group -= 1
            self.show_current_group()
            self.update_navigation()
    
    def next_group(self):
        """Volgende groep"""
        if self.current_group < len(self.duplicates) - 1:
            self.current_group += 1
            self.show_current_group()
            self.update_navigation()
    
    def update_navigation(self):
        """Update navigatie"""
        if self.duplicates:
            self.group_label.setText(f"Groep {self.current_group + 1} van {len(self.duplicates)}")
            self.prev_btn.setEnabled(self.current_group > 0)
            self.next_btn.setEnabled(self.current_group < len(self.duplicates) - 1)
        else:
            self.group_label.setText("Groep 0 van 0")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
    
    def show_current_group(self):
        """Toon huidige groep"""
        # Clear layout
        while self.images_layout.count():
            item = self.images_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        if not self.duplicates or self.current_group >= len(self.duplicates):
            return
        
        # Toon afbeeldingen
        images = self.duplicates[self.current_group]
        for i, img_data in enumerate(images):
            card = ImageCard(img_data)
            card.deleteRequested.connect(self.delete_image)
            
            row, col = divmod(i, 4)  # 4 kolommen
            self.images_layout.addWidget(card, row, col)
    
    def delete_image(self, file_path):
        """Verwijder afbeelding"""
        reply = QMessageBox.question(
            self, 
            "Bevestigen",
            f"Weet je zeker dat je '{os.path.basename(file_path)}' wilt verwijderen?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                # Verwijder bestand
                if os.path.exists(file_path):
                    os.remove(file_path)
                
                # Update database
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("UPDATE images SET is_deleted = TRUE WHERE path = ?", (file_path,))
                
                QMessageBox.information(self, "Verwijderd", f"Bestand verwijderd: {os.path.basename(file_path)}")
                
                # Herlaad resultaten
                self.load_existing_results()
                
            except Exception as e:
                QMessageBox.critical(self, "Fout", f"Kon bestand niet verwijderen: {str(e)}")

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    window = DuplicatePhotoManager()
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()