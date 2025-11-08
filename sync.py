#!/usr/bin/env python3
"""
SQL Anywhere to Web API Sync Tool (Simplified)
-------------------------------------------------
This version only handles:
  1️⃣ acc_users table (Users)
  2️⃣ acc_master table (Debtors)

Removes all other sync features.
"""

import json
import logging
import os
import sys
import traceback
from typing import List, Dict, Any, Optional
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ========================== CONFIG ==========================
class DatabaseConfig:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Configuration file '{self.config_file}' not found!")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in config file: {e}")
            sys.exit(1)

    @property
    def dsn(self): return self.config["database"]["dsn"]
    @property
    def username(self): return self.config["database"]["username"]
    @property
    def password(self): return self.config["database"]["password"]
    @property
    def api_base_url(self): return self.config["api"]["base_url"]
    @property
    def api_timeout(self): return self.config["api"].get("timeout", 120)
    @property
    def client_id(self): return self.config["settings"]["client_id"]
    @property
    def table_name_users(self): return self.config["settings"].get("table_name_users", "acc_users")
    @property
    def table_name_acc_master(self): return self.config["settings"].get("table_name_acc_master", "acc_master")
    @property
    def log_level(self): return self.config["settings"].get("log_level", "INFO")


# ========================== DB CONNECTOR ==========================
class DatabaseConnector:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection: Optional[pyodbc.Connection] = None

    def connect(self) -> bool:
        try:
            conn_str = f"DSN={self.config.dsn};UID={self.config.username};PWD={self.config.password};"
            logging.info(f"Connecting to database DSN: {self.config.dsn}")
            self.connection = pyodbc.connect(conn_str, timeout=10)
            logging.info("✅ Database connected")
            return True
        except pyodbc.Error as e:
            logging.error(f"❌ Database connection failed: {e}")
            print(f"❌ Database connection failed: {e}")
            return False

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                logging.info("🔒 Database connection closed")
            except Exception as e:
                logging.warning(f"⚠️ Error closing connection: {e}")
            finally:
                self.connection = None

    # ✅ ADD THESE TWO METHODS for "with self.db:" support
    def __enter__(self):
        connected = self.connect()
        if not connected:
            raise RuntimeError("Database connection failed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _cursor(self):
        if not self.connection:
            raise RuntimeError("Database not connected")
        return self.connection.cursor()

    def fetch_users(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch records from acc_users"""
        cursor = None
        try:
            cursor = self._cursor()
            query = f"SELECT id, pass, role, accountcode FROM {self.config.table_name_users}"
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Error fetching users: {e}\n{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                cursor.close()

    def fetch_acc_master(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch debtors (acc_master)"""
        cursor = None
        try:
            cursor = self._cursor()
            query = f"""
                SELECT code, name, super_code, opening_balance, debit, credit,
                       place, phone2, openingdepartment, area
                FROM {self.config.table_name_acc_master}
                WHERE super_code IN ('DEBTO', 'SUNCR');
            """
            logging.info("Fetching acc_master (Debtors)...")
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            logging.info(f"✅ {len(rows)} debtors fetched")
            return rows
        except Exception as e:
            logging.error(f"❌ Error fetching acc_master: {e}\n{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                cursor.close()


# ========================== API CLIENT ==========================
class WebAPIClient:
    ENDPOINT_USERS = "/upload-users/"
    ENDPOINT_ACC_MASTER = "/upload-acc-master/"

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({'Content-Type': 'application/json'})
        return session

    def upload_users(self, users: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.ENDPOINT_USERS}?client_id={self.config.client_id}"
        try:
            logging.info(f"📤 Uploading {len(users)} users...")
            res = self.session.post(url, json=users, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Users uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_users: {e}")
            return False

    def upload_acc_master(self, acc_master: List[Dict[str, Any]]) -> bool:
        if not acc_master:
            logging.warning("⚠️ No acc_master data to upload")
            return True

        url = f"{self.config.api_base_url}{self.ENDPOINT_ACC_MASTER}?client_id={self.config.client_id}&force_clear=true"
        batch_size = 200  # You can adjust this if needed

        try:
            total = len(acc_master)
            logging.info(f"📦 Uploading {total} acc_master records in batches of {batch_size}...")

            for i in range(0, total, batch_size):
                batch = acc_master[i:i + batch_size]
                batch_url = url + ("&append=true" if i > 0 else "")
                logging.info(f"📤 Uploading batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} ({len(batch)} records)")

                res = self.session.post(batch_url, json=batch, timeout=self.config.api_timeout)
                if res.status_code not in [200, 201]:
                    logging.error(f"❌ Batch failed: {res.status_code} - {res.text}")
                    return False

            logging.info("✅ All batches uploaded successfully")
            return True
        except Exception as e:
            logging.error(f"❌ Exception in upload_acc_master: {e}")
            return False



# ========================== MAIN SYNC TOOL ==========================
class SyncTool:
    def __init__(self):
        self.config = None
        self.db = None
        self.api = None
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )

    def initialize(self) -> bool:
        try:
            self.config = DatabaseConfig()
            self.db = DatabaseConnector(self.config)
            self.api = WebAPIClient(self.config)
            return True
        except Exception as e:
            logging.error(f"Initialization failed: {e}")
            return False

    def run(self):
        if not self.initialize():
            return

        with self.db:
            # 1️⃣ Sync Users
            users = self.db.fetch_users()
            if users:
                formatted_users = [
                    {
                        "id": u["id"],
                        "pass": u["pass"],
                        "role": u.get("role"),
                        "accountcode": u.get("accountcode")
                    }
                    for u in users
                ]
                self.api.upload_users(formatted_users)
            else:
                logging.warning("⚠️ No users found to upload")

            # 2️⃣ Sync Debtors (acc_master)
            acc_master = self.db.fetch_acc_master()
            if acc_master:
                formatted_acc = [
                    {
                        "code": a["code"],
                        "name": a.get("name"),
                        "super_code": a.get("super_code"),
                        "opening_balance": float(a["opening_balance"]) if a.get("opening_balance") else None,
                        "debit": float(a["debit"]) if a.get("debit") else None,
                        "credit": float(a["credit"]) if a.get("credit") else None,
                        "place": a.get("place"),
                        "phone2": a.get("phone2"),
                        "openingdepartment": a.get("openingdepartment"),
                        "area": a.get("area"),
                    }
                    for a in acc_master
                ]
                self.api.upload_acc_master(formatted_acc)
            else:
                logging.warning("⚠️ No debtors found to upload")


# ========================== ENTRY POINT ==========================
if __name__ == "__main__":
    tool = SyncTool()
    tool.run()
