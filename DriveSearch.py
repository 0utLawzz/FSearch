#!/usr/bin/env python3
"""
Google Drive File Search and Metadata Extractor
Searches through Google Drive Desktop synced folder and lists all files with detailed metadata
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import json
import argparse
import pickle

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False

# Formatting is optional
try:
    from gspread.formatting import cell_format, color, textFormat
    GSPREAD_FORMATTING_AVAILABLE = True
except ImportError:
    GSPREAD_FORMATTING_AVAILABLE = False

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials as OAuthCredentials
    GOOGLE_DRIVE_API_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_API_AVAILABLE = False

# Google Sheets Configuration
GOOGLE_SHEET_ID = "1YrrqKaeslySq565uwKOzEQJKEVzVmASzNmOn8yx0W-s"

LOCAL_CONFIG = {
    "sheet_id": GOOGLE_SHEET_ID,
    "sheet_name": "LSheet",
    "paths": {
        "clients": "F:\\Brandex004\\My Drive\\1 ALL CLIENTS",
        "consultants": "F:\\Brandex004\\My Drive\\2 CONSULTANTS",
    },
}

OAUTH_CONFIG = {
    "sheet_id": GOOGLE_SHEET_ID,
    "sheet_name": "ASheet",
    "folder_ids": {
        "clients": "18T0MojE1IiT7uIz9P8Sthlamj8icR8X6",
        "consultants": "1Ke_B9vI_DdiiXPTTCBDtS4Ny6l73kIBU",
    },
}

OAUTH_CREDENTIALS_FILE = "credentials.json"
OAUTH_TOKEN_FILE = "token.pickle"
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

OAUTH_FOLDER_IDS = OAUTH_CONFIG["folder_ids"]

class DriveSearcher:
    def __init__(self, drive_path: Optional[str] = None):
        """
        Initialize the Drive Searcher
        
        Args:
            drive_path: Path to Google Drive folder. If None, tries common locations.
                        Can be pipe-delimited for multiple paths (e.g., "path1|path2").
        """
        # Delay local path prompting until Local mode is selected.
        self.drive_paths = []
        self.drive_path = ''

        if drive_path:
            if '|' in drive_path:
                paths = drive_path.split('|')
            else:
                paths = [drive_path]

            existing = [p for p in paths if os.path.exists(p)]
            if existing:
                self.drive_paths = existing
                self.drive_path = existing[0]
        self.files_data = []

    def _build_duplicate_key(self, item: Dict, mode: str, key_mode: str) -> str:
        if mode == 'oauth':
            name = str(item.get('name', ''))
            size = str(item.get('size', ''))
            mime = str(item.get('mimeType', ''))
            md5 = str(item.get('md5Checksum', ''))
            parents = str(item.get('parents', ''))
            if key_mode == 'path':
                return f"{parents}|{name}|{mime}|{size}"
            if key_mode == 'name':
                return f"{name}|{mime}"
            if key_mode == 'name_size':
                return f"{name}|{mime}|{size}"
            if key_mode == 'name_size_md5':
                return f"{name}|{mime}|{size}|{md5}"
            return f"{name}|{mime}|{size}|{md5}"

        name = str(item.get('name', ''))
        size = str(item.get('size_mb', ''))
        rel = str(item.get('relative_path', ''))
        ext = str(item.get('extension', ''))
        if key_mode == 'path':
            return rel
        if key_mode == 'name':
            return f"{name}|{ext}"
        if key_mode == 'name_size':
            return f"{name}|{ext}|{size}"
        if key_mode == 'name_size_md5':
            return f"{name}|{ext}|{size}"
        return rel

    def handle_duplicates(self, mode: str, strategy: str = 'keep', key_mode: str = 'name_size_md5'):
        """Handle duplicates in self.files_data.

        strategy:
          - keep: do nothing
          - mark: add fields is_duplicate, duplicate_group, duplicate_index, duplicate_total, duplicate_key
          - remove: keep first item per duplicate_key (still marks)
        """
        if not self.files_data:
            return

        if strategy not in {'keep', 'mark', 'remove'}:
            return

        if strategy == 'keep':
            return

        groups: Dict[str, List[Dict]] = {}
        for item in self.files_data:
            k = self._build_duplicate_key(item, mode=mode, key_mode=key_mode)
            groups.setdefault(k, []).append(item)

        group_num = 0
        for k, items in groups.items():
            if len(items) <= 1:
                it = items[0]
                it['duplicate_key'] = k
                it['is_duplicate'] = False
                it['duplicate_group'] = ''
                it['duplicate_index'] = 1
                it['duplicate_total'] = 1
                continue

            group_num += 1
            for idx, it in enumerate(items, start=1):
                it['duplicate_key'] = k
                it['is_duplicate'] = idx > 1
                it['duplicate_group'] = f"DUP{group_num}"
                it['duplicate_index'] = idx
                it['duplicate_total'] = len(items)

        if strategy == 'remove':
            new_data = []
            for k, items in groups.items():
                new_data.append(items[0])
            self.files_data = new_data

    def _select_mode_menu(self) -> str:
        print("\n" + "="*60)
        print("SELECT MODE")
        print("="*60)
        print("\nAvailable modes:")
        print("  1. Run Local mode")
        print("  2. Run OAuth mode")
        while True:
            try:
                choice = input("\nSelect option (1-2): ").strip()
                if choice == "1":
                    return "local"
                if choice == "2":
                    return "oauth"
                print("Invalid option. Please enter 1-2.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(0)

    def _select_target_menu(self) -> str:
        """Select clients / consultants / both. Returns label: clients|consultants|both"""
        print("\n" + "="*60)
        print("SELECT TARGET")
        print("="*60)

        path1 = LOCAL_CONFIG["paths"]["clients"]
        path2 = LOCAL_CONFIG["paths"]["consultants"]

        print("\nAvailable paths:")
        exists1 = "✓" if os.path.exists(path1) else "✗"
        exists2 = "✓" if os.path.exists(path2) else "✗"
        print(f"  1. [{exists1}] {path1}")
        print(f"  2. [{exists2}] {path2}")
        print("  3. Both paths")

        while True:
            try:
                choice = input("\nSelect option (1-3): ").strip()
                if choice == '1':
                    return 'clients'
                if choice == '2':
                    return 'consultants'
                if choice == '3':
                    return 'both'
                print("Invalid option. Please enter 1-3.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(0)

    def _select_duplicate_menu(self) -> str:
        """Duplicate check menu. Returns 'mark' or 'keep'."""
        print("\n" + "="*60)
        print("DUPLICATE CHECK")
        print("="*60)
        print("\n  1. YES")
        print("  2. NO")
        while True:
            try:
                choice = input("\nSelect option (1-2): ").strip()
                if choice == '1':
                    return 'mark'
                if choice == '2':
                    return 'keep'
                print("Invalid option. Please enter 1-2.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(0)
        
    def _select_path_menu(self) -> str:
        """Display menu to select or enter a local path"""
        print("\n" + "="*60)
        print("SELECT LOCAL PATH TO SEARCH")
        print("="*60)
        
        # Specific paths to search
        path1 = LOCAL_CONFIG["paths"]["clients"]
        path2 = LOCAL_CONFIG["paths"]["consultants"]
        
        # Display available options
        print("\nAvailable paths:")
        exists1 = "✓" if os.path.exists(path1) else "✗"
        exists2 = "✓" if os.path.exists(path2) else "✗"
        print(f"  1. [{exists1}] {path1}")
        print(f"  2. [{exists2}] {path2}")
        print(f"  3. Both paths")
        
        while True:
            try:
                choice = input("\nSelect option (1-3): ").strip()
                choice_num = int(choice)
                
                if choice_num == 1:
                    if os.path.exists(path1):
                        print(f"Selected: {path1}")
                        return path1
                    else:
                        print(f"Error: Path does not exist: {path1}")
                        print("Please select another option.")
                elif choice_num == 2:
                    if os.path.exists(path2):
                        print(f"Selected: {path2}")
                        return path2
                    else:
                        print(f"Error: Path does not exist: {path2}")
                        print("Please select another option.")
                elif choice_num == 3:
                    if os.path.exists(path1) and os.path.exists(path2):
                        print(f"Selected: Both paths")
                        return f"{path1}|{path2}"  # Use delimiter to indicate both
                    else:
                        missing = []
                        if not os.path.exists(path1):
                            missing.append(path1)
                        if not os.path.exists(path2):
                            missing.append(path2)
                        print(f"Error: Path(s) do not exist: {', '.join(missing)}")
                        print("Please select another option.")
                else:
                    print("Invalid option. Please enter 1-3.")
            except ValueError:
                print("Invalid input. Please enter a number.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(0)

    def _find_drive_path(self, drive_path: Optional[str] = None) -> str:
        """Find Google Drive folder path"""
        if drive_path and os.path.exists(drive_path):
            return drive_path
            
        # If no path provided, show menu
        return self._select_path_menu()

    def set_local_paths_from_target(self, target: str):
        if target == 'clients':
            self.drive_paths = [LOCAL_CONFIG["paths"]["clients"]]
        elif target == 'consultants':
            self.drive_paths = [LOCAL_CONFIG["paths"]["consultants"]]
        else:
            self.drive_paths = [LOCAL_CONFIG["paths"]["clients"], LOCAL_CONFIG["paths"]["consultants"]]
        self.drive_path = self.drive_paths[0] if self.drive_paths else ''
    
    def _get_file_metadata(self, file_path: Path, base_path: Optional[str] = None, root_label: Optional[str] = None) -> Dict:
        """Extract comprehensive metadata for a file"""
        try:
            stat = file_path.stat()
            # Use provided base_path or fall back to self.drive_path
            actual_base_path = base_path if base_path else self.drive_path
            relative_path = str(file_path.relative_to(actual_base_path))
            
            # Trim base path from full path (remove F:\Brandex004\My Drive\)
            full_path = str(file_path)
            base_prefix = r"F:\Brandex004\My Drive\\"
            trimmed_path = full_path.replace(base_prefix.rstrip("\\"), "") if full_path.startswith(base_prefix.rstrip("\\")) else full_path
            
            # Split relative path into levels
            path_parts = relative_path.split('\\')
            level_file = file_path.stem  # filename without extension
            
            return {
                'root_label': root_label or '',
                'name': file_path.name,
                'path': trimmed_path,
                'relative_path': relative_path,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'extension': file_path.suffix.lower() if file_path.suffix else 'NO_EXTENSION',
                'file_type': self._get_file_type(file_path.suffix),
                'created_date': datetime.fromtimestamp(stat.st_ctime).strftime('%d-%b-%y'),
                'modified_date': datetime.fromtimestamp(stat.st_mtime).strftime('%d-%b-%y'),
                'accessed_date': datetime.fromtimestamp(stat.st_atime).strftime('%d-%b-%y'),
                'is_directory': file_path.is_dir(),
                'is_hidden': file_path.name.startswith('.'),
                'parent_folder': file_path.parent.name,
                'depth': len(file_path.parts) - len(Path(actual_base_path).parts),
                'LEVEL1': path_parts[0] if len(path_parts) > 0 else '',
                'LEVEL2': path_parts[1] if len(path_parts) > 1 else '',
                'LEVEL3': path_parts[2] if len(path_parts) > 2 else '',
                'LEVEL4': path_parts[3] if len(path_parts) > 3 else '',
                'LEVEL5': path_parts[4] if len(path_parts) > 4 else '',
                'LEVEL_FILE': level_file
            }
        except (OSError, PermissionError) as e:
            return {
                'name': file_path.name,
                'path': str(file_path),
                'error': f"Access denied: {str(e)}"
            }
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    def _get_file_type(self, extension: str) -> str:
        """Categorize file type based on extension"""
        extension = extension.lower()
        
        type_mapping = {
            # Documents
            '.pdf': 'PDF Document',
            '.doc': 'Word Document', '.docx': 'Word Document',
            '.xls': 'Excel Spreadsheet', '.xlsx': 'Excel Spreadsheet',
            '.ppt': 'PowerPoint', '.pptx': 'PowerPoint',
            '.txt': 'Text File', '.rtf': 'Rich Text',
            '.odt': 'OpenDocument Text', '.ods': 'OpenDocument Spreadsheet',
            
            # Images
            '.jpg': 'Image', '.jpeg': 'Image', '.png': 'Image', '.gif': 'Image',
            '.bmp': 'Image', '.tiff': 'Image', '.svg': 'Image', '.webp': 'Image',
            '.ico': 'Icon', '.psd': 'Photoshop File',
            
            # Videos
            '.mp4': 'Video', '.avi': 'Video', '.mkv': 'Video', '.mov': 'Video',
            '.wmv': 'Video', '.flv': 'Video', '.webm': 'Video', '.m4v': 'Video',
            
            # Audio
            '.mp3': 'Audio', '.wav': 'Audio', '.flac': 'Audio', '.aac': 'Audio',
            '.ogg': 'Audio', '.wma': 'Audio', '.m4a': 'Audio',
            
            # Archives
            '.zip': 'Archive', '.rar': 'Archive', '.7z': 'Archive',
            '.tar': 'Archive', '.gz': 'Archive', '.bz2': 'Archive',
            
            # Code
            '.py': 'Python Code', '.js': 'JavaScript', '.html': 'HTML',
            '.css': 'CSS', '.java': 'Java', '.cpp': 'C++', '.c': 'C',
            '.php': 'PHP', '.rb': 'Ruby', '.go': 'Go', '.rs': 'Rust',
            
            # Other
            '.exe': 'Executable', '.msi': 'Installer', '.dll': 'Library',
            '.iso': 'Disk Image', '.db': 'Database', '.sql': 'SQL File'
        }
        
        return type_mapping.get(extension, f'{extension.upper()} File' if extension else 'Folder')
    
    def search_drive(self, 
                    extension_filter: Optional[str] = None,
                    name_filter: Optional[str] = None,
                    min_size: Optional[int] = None,
                    max_size: Optional[int] = None,
                    include_dirs: bool = True) -> List[Dict]:
        """
        Search through Google Drive folder
        
        Args:
            extension_filter: Filter by file extension (e.g., '.pdf', 'pdf')
            name_filter: Filter by filename (contains)
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            include_dirs: Include directories in results
            
        Returns:
            List of file metadata dictionaries
        """
        print(f"Searching Google Drive at: {self.drive_path}")
        print("This may take a while for large drives...")
        
        # Normalize extension filter
        if extension_filter and not extension_filter.startswith('.'):
            extension_filter = '.' + extension_filter.lower()
        
        # Extensions to exclude
        excluded_extensions = {'.ini', '.tmp'}
        
        files_found = 0
        
        local_label_by_path = {
            LOCAL_CONFIG["paths"]["clients"]: "clients",
            LOCAL_CONFIG["paths"]["consultants"]: "consultants",
        }

        for search_path_str in self.drive_paths:
            print(f"Searching: {search_path_str}")
            search_path = Path(search_path_str)
            root_label = local_label_by_path.get(search_path_str, '')
            
            try:
                for item in search_path.rglob('*'):
                    try:
                        # Skip if we don't want directories and this is a directory
                        if item.is_dir() and not include_dirs:
                            continue
                        
                        # Exclude certain file types
                        if item.suffix.lower() in excluded_extensions:
                            continue
                        
                        # Apply filters
                        if extension_filter and item.suffix.lower() != extension_filter:
                            continue
                        if name_filter and name_filter.lower() not in item.name.lower():
                            continue
                        
                        metadata = self._get_file_metadata(item, base_path=search_path_str, root_label=root_label)
                        
                        # Size filters (skip for directories) - convert bytes to MB
                        if not item.is_dir():
                            min_size_mb = min_size / (1024 * 1024) if min_size else None
                            max_size_mb = max_size / (1024 * 1024) if max_size else None
                            if min_size_mb and metadata.get('size_mb', 0) < min_size_mb:
                                continue
                            if max_size_mb and metadata.get('size_mb', 0) > max_size_mb:
                                continue
                        
                        self.files_data.append(metadata)
                        files_found += 1
                        
                        # Progress indicator
                        if files_found % 100 == 0:
                            print(f"Found {files_found} items...")
                            
                    except (PermissionError, OSError) as e:
                        print(f"Skipping {item}: {e}")
                        continue
                        
            except KeyboardInterrupt:
                print(f"\nSearch interrupted. Found {files_found} items so far.")
                break
        
        print(f"Search completed. Found {len(self.files_data)} items.")
        return self.files_data

    def _get_oauth_credentials(self):
        if not GOOGLE_DRIVE_API_AVAILABLE:
            raise RuntimeError(
                "Google Drive API libraries not installed. Install with: pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2"
            )

        creds = None
        if os.path.exists(OAUTH_TOKEN_FILE):
            with open(OAUTH_TOKEN_FILE, "rb") as token:
                creds = pickle.load(token)

        if not creds or not getattr(creds, "valid", False):
            if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
                creds.refresh(Request())
            else:
                if not os.path.exists(OAUTH_CREDENTIALS_FILE):
                    raise FileNotFoundError(
                        f"OAuth credentials file not found: {OAUTH_CREDENTIALS_FILE}. Download it from Google Cloud Console (OAuth Client ID -> Desktop app)"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDENTIALS_FILE, OAUTH_SCOPES)
                creds = flow.run_local_server(port=0)

            with open(OAUTH_TOKEN_FILE, "wb") as token:
                pickle.dump(creds, token)

        return creds

    def _get_drive_service(self):
        creds = self._get_oauth_credentials()
        return build("drive", "v3", credentials=creds)

    def _drive_item_to_metadata(self, item: Dict, parent_folder_id: Optional[str] = None, root_label: Optional[str] = None) -> Dict:
        owners = item.get("owners") or []
        owner_emails = ",".join([o.get("emailAddress", "") for o in owners if o])
        owner_names = ",".join([o.get("displayName", "") for o in owners if o])
        parents = item.get("parents") or []
        return {
            "id": item.get("id", ""),
            "name": item.get("name", ""),
            "mimeType": item.get("mimeType", ""),
            "is_folder": item.get("mimeType") == "application/vnd.google-apps.folder",
            "size": item.get("size", ""),
            "createdTime": item.get("createdTime", ""),
            "modifiedTime": item.get("modifiedTime", ""),
            "trashed": item.get("trashed", ""),
            "starred": item.get("starred", ""),
            "shared": item.get("shared", ""),
            "ownedByMe": item.get("ownedByMe", ""),
            "owners": owner_names,
            "ownerEmails": owner_emails,
            "lastModifyingUser": (item.get("lastModifyingUser") or {}).get("displayName", ""),
            "lastModifyingUserEmail": (item.get("lastModifyingUser") or {}).get("emailAddress", ""),
            "parents": ",".join(parents),
            "parent_folder_id": parent_folder_id or (parents[0] if parents else ""),
            "root_label": root_label or "",
            "webViewLink": item.get("webViewLink", ""),
            "webContentLink": item.get("webContentLink", ""),
            "iconLink": item.get("iconLink", ""),
            "md5Checksum": item.get("md5Checksum", ""),
            "sha1Checksum": item.get("sha1Checksum", ""),
            "sha256Checksum": item.get("sha256Checksum", ""),
            "driveId": item.get("driveId", ""),
            "teamDriveId": item.get("teamDriveId", ""),
            "shortcutTargetId": ((item.get("shortcutDetails") or {}).get("targetId")) or "",
            "shortcutTargetMimeType": ((item.get("shortcutDetails") or {}).get("targetMimeType")) or "",
        }

    def search_drive_oauth(self, folder_ids: Dict[str, str]) -> List[Dict]:
        self.files_data = []
        service = self._get_drive_service()

        fields = (
            "nextPageToken, files("
            "id,name,mimeType,size,createdTime,modifiedTime,trashed,starred,shared,ownedByMe,"
            "owners(displayName,emailAddress),lastModifyingUser(displayName,emailAddress),"
            "parents,webViewLink,webContentLink,iconLink,md5Checksum,sha1Checksum,sha256Checksum,"
            "driveId,teamDriveId,shortcutDetails)"
        )

        total_found = 0
        for label, root_id in folder_ids.items():
            print(f"Searching (OAuth) folder '{label}' with id: {root_id}")
            stack = [root_id]
            while stack:
                current_folder_id = stack.pop()
                page_token = None
                while True:
                    try:
                        resp = service.files().list(
                            q=f"'{current_folder_id}' in parents and trashed=false",
                            fields=fields,
                            pageSize=1000,
                            pageToken=page_token,
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        ).execute()
                    except HttpError as e:
                        print(f"Drive API error for folder {current_folder_id}: {e}")
                        break

                    for f in resp.get("files", []):
                        meta = self._drive_item_to_metadata(f, parent_folder_id=current_folder_id, root_label=label)
                        self.files_data.append(meta)
                        total_found += 1
                        if meta.get("is_folder"):
                            stack.append(meta.get("id"))

                        if total_found % 200 == 0:
                            print(f"Found {total_found} items...")

                    page_token = resp.get("nextPageToken")
                    if not page_token:
                        break

        print(f"Search completed. Found {len(self.files_data)} items.")
        return self.files_data
    
    def display_results(self, 
                       sort_by: str = 'name',
                       reverse: bool = False,
                       show_details: bool = True):
        """Display search results in formatted table"""
        if not self.files_data:
            print("No files found.")
            return

        is_oauth_mode = any(('mimeType' in f or 'id' in f) for f in self.files_data)
        if is_oauth_mode:
            try:
                self.files_data.sort(key=lambda x: str(x.get(sort_by, x.get('name', ''))), reverse=reverse)
            except TypeError:
                self.files_data.sort(key=lambda x: str(x.get('name', '')), reverse=reverse)

            print(f"\n{'='*120}")
            print(f"GOOGLE DRIVE API SEARCH RESULTS - {len(self.files_data)} items found")
            print(f"{'='*120}")

            if show_details:
                print(f"{'Name':<45} {'MIME Type':<35} {'Size':<12} {'Modified':<25} {'Root'}")
                print(f"{'-'*45} {'-'*35} {'-'*12} {'-'*25} {'-'*10}")
                for file_data in self.files_data:
                    name = (file_data.get('name', '') or '')
                    mime = (file_data.get('mimeType', '') or '')
                    size = file_data.get('size', '')
                    modified = file_data.get('modifiedTime', '')
                    root = file_data.get('root_label', '')
                    name_disp = name[:42] + '...' if len(name) > 45 else name
                    mime_disp = mime[:32] + '...' if len(mime) > 35 else mime
                    print(f"{name_disp:<45} {mime_disp:<35} {str(size):<12} {modified:<25} {root}")
            else:
                for file_data in self.files_data:
                    print(file_data.get('name', ''))
            return
        
        # Sort results
        sort_key_map = {
            'name': 'name',
            'size': 'size_mb',
            'modified': 'modified_date',
            'created': 'created_date',
            'type': 'file_type',
            'extension': 'extension'
        }
        
        key = sort_key_map.get(sort_by, 'name')
        try:
            self.files_data.sort(key=lambda x: x.get(key, ''), reverse=reverse)
        except TypeError:
            # Handle mixed types in sorting
            self.files_data.sort(key=lambda x: str(x.get(key, '')), reverse=reverse)
        
        print(f"\n{'='*120}")
        print(f"GOOGLE DRIVE SEARCH RESULTS - {len(self.files_data)} items found")
        print(f"{'='*120}")
        
        if show_details:
            # Detailed view
            print(f"{'Name':<40} {'Type':<20} {'Size':<12} {'Modified':<20} {'Path'}")
            print(f"{'-'*40} {'-'*20} {'-'*12} {'-'*20} {'-'*50}")
            
            for file_data in self.files_data:
                if 'error' in file_data:
                    print(f"{file_data['name']:<40} {'ERROR':<20} {'N/A':<12} {'N/A':<20} {file_data['error']}")
                else:
                    name = file_data['name'][:37] + '...' if len(file_data['name']) > 40 else file_data['name']
                    path = file_data['relative_path'][:47] + '...' if len(file_data['relative_path']) > 50 else file_data['relative_path']
                    print(f"{name:<40} {file_data['file_type']:<20} {file_data['size_mb']:<12} {file_data['modified_date']:<20} {path}")
        else:
            # Simple view
            for file_data in self.files_data:
                if 'error' not in file_data:
                    print(f"{file_data['relative_path']}")

    def export_results(self, output_file: str, format: str = 'json'):
        """Export search results to file"""
        if not self.files_data:
            print("No data to export.")
            return
        
        if format.lower() == 'json':
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.files_data, f, indent=2, ensure_ascii=False)
        elif format.lower() == 'csv':
            import csv
            if self.files_data:
                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.files_data[0].keys())
                    writer.writeheader()
                    writer.writerows(self.files_data)
        elif format.lower() == 'txt':
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"Google Drive Search Results - {len(self.files_data)} items\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
                for file_data in self.files_data:
                    f.write(f"Name: {file_data.get('name', 'N/A')}\n")
                    f.write(f"Path: {file_data.get('path', 'N/A')}\n")
                    f.write(f"Size: {file_data.get('size_mb', 'N/A')} MB\n")
                    f.write(f"Type: {file_data.get('file_type', 'N/A')}\n")
                    f.write(f"Modified: {file_data.get('modified_date', 'N/A')}\n")
                    f.write("-" * 40 + "\n")
        
        print(f"Results exported to: {output_file}")
    
    def upload_to_google_sheets(self, sheet_id: str = None, service_account_file: str = 'service-account.json', sheet_name: str = None, column_order: Optional[List[str]] = None):
        """
        Upload search results to Google Sheets

        Args:
            sheet_id: Google Sheet ID (defaults to GOOGLE_SHEET_ID from config)
            service_account_file: Path to service account JSON file
        """
        if not GOOGLE_SHEETS_AVAILABLE:
            print("Error: gspread library not installed. Install with: pip install gspread google-auth")
            return False

        if not self.files_data:
            print("No data to upload.")
            return False

        # Use configured sheet ID if not provided
        if sheet_id is None:
            sheet_id = GOOGLE_SHEET_ID

        try:
            # Authenticate with Google Sheets
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(service_account_file, scopes=scope)
            client = gspread.authorize(creds)

            # Open the spreadsheet
            sheet = client.open_by_key(sheet_id)

            # Try to get the worksheet, create it if it doesn't exist
            worksheet_name_to_use = sheet_name if sheet_name else LOCAL_CONFIG["sheet_name"]

            try:
                worksheet = sheet.worksheet(worksheet_name_to_use)
                print(f"Found existing worksheet: {worksheet_name_to_use}")
            except gspread.WorksheetNotFound:
                print(f"Worksheet '{worksheet_name_to_use}' not found. Creating it...")
                worksheet = sheet.add_worksheet(title=worksheet_name_to_use, rows="1000", cols="50")

            # Prepare data with specific column order
            if column_order is None:
                column_order = [
                    'name', 'extension', 'file_type', 'is_directory', 'is_hidden',
                    'parent_folder', 'depth', 'LEVEL1', 'LEVEL2', 'LEVEL3', 'LEVEL4', 'LEVEL5',
                    'LEVEL_FILE', 'created_date', 'modified_date', 'accessed_date', 'path',
                    'relative_path', 'size_mb'
                ]

            # Add emojis to header
            header_with_emoji = []
            emoji_map = {
                'name': '📄 Name',
                'extension': '🔤 Ext',
                'file_type': '📁 Type',
                'is_directory': '📂 Dir',
                'is_hidden': '👁️ Hidden',
                'parent_folder': '📂 Parent',
                'depth': '📊 Depth',
                'LEVEL1': '1️⃣ L1',
                'LEVEL2': '2️⃣ L2',
                'LEVEL3': '3️⃣ L3',
                'LEVEL4': '4️⃣ L4',
                'LEVEL5': '5️⃣ L5',
                'LEVEL_FILE': '📝 File',
                'created_date': '📅 Created',
                'modified_date': '✏️ Modified',
                'accessed_date': '👀 Accessed',
                'path': '📍 Path',
                'relative_path': '🔗 Rel Path',
                'size_mb': '💾 Size MB',
                'root_label': '🏷️ Root',
                'duplicate_group': '🔁 Dup Group',
                'is_duplicate': '⚠️ Is Dup',
                'duplicate_index': '🔢 Dup #',
                'duplicate_total': '📊 Dup Total',
                'duplicate_key': '🔑 Dup Key',
                'parent_folder_id': '📂 Parent ID',
                'parents': '🔗 Parents',
                'id': '🆔 ID',
                'mimeType': '📄 MIME',
                'is_folder': '📂 Folder',
                'size': '💾 Size',
                'createdTime': '📅 Created',
                'modifiedTime': '✏️ Modified',
                'owners': '👤 Owners',
                'ownerEmails': '📧 Owner Emails',
                'lastModifyingUser': '👤 Last Mod By',
                'lastModifyingUserEmail': '📧 Last Mod Email',
                'shared': '🔗 Shared',
                'starred': '⭐ Starred',
                'ownedByMe': '👤 Owned By Me',
                'trashed': '🗑️ Trashed',
                'webViewLink': '🔗 Web Link',
                'webContentLink': '📥 Download Link',
                'iconLink': '🖼️ Icon',
                'md5Checksum': '🔐 MD5',
                'sha1Checksum': '🔐 SHA1',
                'sha256Checksum': '🔐 SHA256',
                'driveId': '🆔 Drive ID',
                'teamDriveId': '🆔 Team Drive ID',
                'shortcutTargetId': '🔗 Shortcut ID',
                'shortcutTargetMimeType': '📄 Shortcut MIME',
            }
            for col in column_order:
                header_with_emoji.append(emoji_map.get(col, col))

            # Check if sheet has any content
            all_values = worksheet.get_all_values()
            is_empty = len(all_values) == 0

            # Prepare data rows
            data = []
            for file_data in self.files_data:
                row = []
                for key in column_order:
                    value = file_data.get(key, '')
                    # Convert boolean values to Yes/No for better readability
                    if isinstance(value, bool):
                        value = 'Yes' if value else 'No'
                    elif value is None:
                        value = ''
                    else:
                        value = str(value)
                    row.append(value)
                data.append(row)

            # Check for duplicates against existing data (skip header row)
            existing_rows = all_values[1:] if len(all_values) > 0 else []
            existing_set = set(tuple(row) for row in existing_rows)
            new_rows = []
            duplicate_count = 0
            for row in data:
                if tuple(row) in existing_set:
                    duplicate_count += 1
                else:
                    new_rows.append(row)

            # Add header only for empty sheet (never rewrite existing sheet)
            if is_empty:
                print("Adding header row with formatting...")
                worksheet.append_row(header_with_emoji, value_input_option='RAW')
                # Format header: bold, background color (if formatting available)
                if GSPREAD_FORMATTING_AVAILABLE:
                    fmt = cell_format(
                        backgroundColor=color(0.2, 0.4, 0.8),
                        textFormat=textFormat(bold=True, fontSize=11)
                    )
                    worksheet.format(f'1:1', fmt)

            # Upload data and report stats
            if new_rows:
                print(f"Uploading {len(new_rows)} new rows to Google Sheets...")
                worksheet.append_rows(new_rows, value_input_option='RAW')
            else:
                print("No new rows to upload (all duplicates).")

            if duplicate_count > 0:
                print(f"Skipped {duplicate_count} duplicate rows.")

            # Freeze the first row (only meaningful if header exists)
            worksheet.freeze(rows=1)

            print(f"✅ Successfully uploaded {len(self.files_data)} items to Google Sheet!")
            print(f"📊 Sheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}")
            return True

        except Exception as e:
            print(f"Error uploading to Google Sheets: {e}")
            return False

    def upload_stats_to_google_sheets(self, stats: Dict, sheet_id: str = None, service_account_file: str = 'service-account.json', sheet_name: str = 'Stats'):
        if not GOOGLE_SHEETS_AVAILABLE:
            print("Error: gspread library not installed. Install with: pip install gspread google-auth")
            return False

        if sheet_id is None:
            sheet_id = GOOGLE_SHEET_ID

        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(service_account_file, scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id)

            try:
                worksheet = sheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=sheet_name, rows="1000", cols="30")

            header = [
                'timestamp', 'mode', 'total_items', 'total_size_mb',
                'qty_clients', 'qty_consultants',
                'duplicates', 'duplicate_key'
            ]

            all_values = worksheet.get_all_values()
            if len(all_values) == 0:
                worksheet.append_row(header, value_input_option='RAW')

            qty = stats.get('quantity_by_root', {}) if stats else {}
            row = [
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                str(stats.get('mode', '')),
                str(stats.get('total_items', '')),
                str(stats.get('total_size_mb', '')),
                str(qty.get('clients', 0)),
                str(qty.get('consultants', 0)),
                str(stats.get('duplicates', '')),
                str(stats.get('duplicate_key', '')),
            ]
            worksheet.append_row(row, value_input_option='RAW')
            return True
        except Exception as e:
            print(f"Error uploading stats to Google Sheets: {e}")
            return False
    
    def get_statistics(self) -> Dict:
        """Get statistics about the search results"""
        if not self.files_data:
            return {}

        is_oauth_mode = any(('mimeType' in f or 'id' in f) for f in self.files_data)
        if is_oauth_mode:
            total_size_bytes = 0
            file_types = {}
            quantity_by_root = {}
            for f in self.files_data:
                mime = f.get('mimeType', 'Unknown')
                file_types[mime] = file_types.get(mime, 0) + 1
                root = f.get('root_label', '')
                quantity_by_root[root] = quantity_by_root.get(root, 0) + 1
                size = f.get('size')
                try:
                    if size not in (None, '') and not f.get('is_folder', False):
                        total_size_bytes += int(size)
                except (TypeError, ValueError):
                    pass

            return {
                'total_items': len(self.files_data),
                'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
                'file_types': file_types,
                'quantity_by_root': quantity_by_root,
                'extensions': {},
                'largest_files': [],
                'newest_files': [],
                'oldest_files': [],
            }

        stats = {
            'total_items': len(self.files_data),
            'total_size_mb': sum(f.get('size_mb', 0) for f in self.files_data if not f.get('is_directory', False)),
            'file_types': {},
            'extensions': {},
            'largest_files': [],
            'newest_files': [],
            'oldest_files': [],
            'quantity_by_root': {}
        }

        # File type and extension counts
        for file_data in self.files_data:
            if not file_data.get('is_directory', False):
                file_type = file_data.get('file_type', 'Unknown')
                extension = file_data.get('extension', 'NO_EXTENSION')

                root = file_data.get('root_label', '')
                stats['quantity_by_root'][root] = stats['quantity_by_root'].get(root, 0) + 1

                stats['file_types'][file_type] = stats['file_types'].get(file_type, 0) + 1
                stats['extensions'][extension] = stats['extensions'].get(extension, 0) + 1

        # Sort files by size and date
        non_dir_files = [f for f in self.files_data if not f.get('is_directory', False) and 'error' not in f]
        stats['largest_files'] = sorted(non_dir_files, key=lambda x: x.get('size_mb', 0), reverse=True)[:10]

        return stats

def main():
    parser = argparse.ArgumentParser(description='Search Google Drive and list file metadata')
    parser.add_argument('--path', help='Path to Google Drive folder')
    parser.add_argument('--mode', choices=['local', 'oauth'], help='Run mode (local or oauth)')
    parser.add_argument('--duplicates', choices=['keep', 'mark', 'remove'], default='keep', help='Duplicate handling (default: keep)')
    parser.add_argument('--duplicate-key', choices=['path', 'name', 'name_size', 'name_size_md5'], default='name_size_md5', help='How to detect duplicates (default: name_size_md5)')
    parser.add_argument('--extension', help='Filter by file extension (e.g., pdf, .pdf)')
    parser.add_argument('--name', help='Filter by filename (contains)')
    parser.add_argument('--min-size', type=int, help='Minimum file size in bytes')
    parser.add_argument('--max-size', type=int, help='Maximum file size in bytes')
    parser.add_argument('--no-dirs', action='store_true', help='Exclude directories from results')
    parser.add_argument('--sort', choices=['name', 'size', 'modified', 'created', 'type', 'extension'], 
                       default='name', help='Sort results by field')
    parser.add_argument('--reverse', action='store_true', help='Reverse sort order')
    parser.add_argument('--simple', action='store_true', help='Show simple file list only')
    parser.add_argument('--export', help='Export results to file (default: drive_files.csv)', default='drive_files.csv')
    parser.add_argument('--format', choices=['json', 'csv', 'txt'], default='csv', 
                       help='Export format (default: csv)')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--no-display', action='store_true', help='Skip displaying results, just export')
    parser.add_argument('--sheet-id', help='Google Sheet ID to upload results to')
    parser.add_argument('--service-account', help='Path to service account JSON file', default='service-account.json')
    
    args = parser.parse_args()
    
    try:
        searcher = DriveSearcher(args.path)

        target = searcher._select_target_menu()
        mode = args.mode if args.mode else searcher._select_mode_menu()

        duplicates_strategy = args.duplicates if args.duplicates != 'keep' else searcher._select_duplicate_menu()
        duplicate_key = args.duplicate_key

        if mode == 'local':
            searcher.set_local_paths_from_target(target)
            results = searcher.search_drive(
                extension_filter=args.extension,
                name_filter=args.name,
                min_size=args.min_size,
                max_size=args.max_size,
                include_dirs=not args.no_dirs
            )
            sheet_name_to_use = LOCAL_CONFIG["sheet_name"]
            column_order_to_use = [
                'root_label',
                'name', 'extension', 'file_type', 'is_directory', 'is_hidden',
                'parent_folder', 'depth', 'LEVEL1', 'LEVEL2', 'LEVEL3', 'LEVEL4', 'LEVEL5',
                'LEVEL_FILE', 'created_date', 'modified_date', 'accessed_date', 'path',
                'relative_path', 'size_mb'
            ]
        else:
            if target == 'clients':
                folder_ids = {'clients': OAUTH_CONFIG['folder_ids']['clients']}
            elif target == 'consultants':
                folder_ids = {'consultants': OAUTH_CONFIG['folder_ids']['consultants']}
            else:
                folder_ids = OAUTH_FOLDER_IDS

            results = searcher.search_drive_oauth(folder_ids)
            sheet_name_to_use = OAUTH_CONFIG["sheet_name"]
            column_order_to_use = [
                'root_label', 'parent_folder_id', 'parents',
                'duplicate_group', 'is_duplicate', 'duplicate_index', 'duplicate_total', 'duplicate_key',
                'id', 'name', 'mimeType', 'is_folder', 'size',
                'createdTime', 'modifiedTime',
                'owners', 'ownerEmails',
                'lastModifyingUser', 'lastModifyingUserEmail',
                'shared', 'starred', 'ownedByMe', 'trashed',
                'webViewLink', 'webContentLink', 'iconLink',
                'md5Checksum', 'sha1Checksum', 'sha256Checksum',
                'driveId', 'teamDriveId',
                'shortcutTargetId', 'shortcutTargetMimeType',
            ]

        if mode == 'local':
            if 'duplicate_group' not in column_order_to_use:
                column_order_to_use = [
                    'duplicate_group', 'is_duplicate', 'duplicate_index', 'duplicate_total', 'duplicate_key'
                ] + column_order_to_use

        searcher.handle_duplicates(mode=mode, strategy=duplicates_strategy, key_mode=duplicate_key)
        
        # Display results unless skipped
        if not args.no_display:
            searcher.display_results(
                sort_by=args.sort,
                reverse=args.reverse,
                show_details=not args.simple
            )
        
        # Always export results
        searcher.export_results(args.export, args.format)
        
        # Show statistics if requested
        if args.stats:
            stats = searcher.get_statistics()
            stats['mode'] = mode
            stats['duplicates'] = duplicates_strategy
            stats['duplicate_key'] = duplicate_key
            print(f"\n{'='*60}")
            print("STATISTICS")
            print(f"{'='*60}")
            print(f"Total items: {stats.get('total_items', 0)}")
            print(f"Total size: {round(stats.get('total_size_mb', 0), 2)} MB")

            qty = stats.get('quantity_by_root', {})
            if qty:
                print(f"\nQuantity (separate):")
                for k, v in sorted(qty.items(), key=lambda x: x[0]):
                    label = k if k else 'unknown'
                    print(f"  {label}: {v}")
            
            print(f"\nTop file types:")
            for file_type, count in sorted(stats['file_types'].items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  {file_type}: {count}")
            
            print(f"\nTop extensions:")
            for ext, count in sorted(stats['extensions'].items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  {ext}: {count}")
        
        print(f"\n✅ Done! Results exported to: {args.export}")
        print(f"📊 Open this file in Excel to view and analyze your Google Drive files.")
            
        # Upload to Google Sheets (use configured sheet ID or provided argument)
        default_sheet_id = LOCAL_CONFIG["sheet_id"] if mode == 'local' else OAUTH_CONFIG["sheet_id"]
        sheet_id_to_use = args.sheet_id if args.sheet_id else default_sheet_id
        print(f"\n📤 Uploading to Google Sheets...")
        searcher.upload_to_google_sheets(
            sheet_id_to_use,
            args.service_account,
            sheet_name=sheet_name_to_use,
            column_order=column_order_to_use,
        )

        if args.stats:
            stats_sheet_name = f"{sheet_name_to_use}_STATS"
            searcher.upload_stats_to_google_sheets(
                stats,
                sheet_id_to_use,
                args.service_account,
                sheet_name=stats_sheet_name,
            )
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
