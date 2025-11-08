#!/usr/bin/env python3
"""
SQL Anywhere to Web API Sync Tool
Connects to SQL Anywhere database via ODBC and syncs data to web API

Updated: Added sales_daywise and sales_monthwise sync functionality
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DatabaseConfig:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Configuration file '{self.config_file}' not found!")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in configuration file: {e}")
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
    def table_name_misel(self): return self.config["settings"].get("table_name_misel", "misel")
    @property
    def batch_size(self): return self.config["settings"].get("batch_size", 1000)
    @property
    def large_table_batch_size(self): return self.config["settings"].get("large_table_batch_size", 500)
    @property
    def log_level(self): return self.config["settings"].get("log_level", "INFO")


class DatabaseConnector:
    """
    Encapsulates the database connection. Provides safe connect/close and context-manager support.
    """
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection: Optional[pyodbc.Connection] = None

    def connect(self) -> bool:
        try:
            conn_str = f"DSN={self.config.dsn};UID={self.config.username};PWD={self.config.password};"
            logging.info(f"Connecting to database DSN: {self.config.dsn}")
            self.connection = pyodbc.connect(conn_str, timeout=10)
            logging.info("✅ Successfully connected to database")
            return True
        except pyodbc.Error as e:
            logging.error(f"❌ Database connection failed: {e}")
            print(f"❌ Failed to connect to database: {e}")
            return False

    def close(self):
        """
        Safely close the connection.
        """
        try:
            if self.connection is not None:
                try:
                    self.connection.close()
                    logging.info("🔒 Database connection closed")
                except Exception as e:
                    logging.warning(f"⚠️ Error while closing connection: {e}")
                finally:
                    self.connection = None
            else:
                logging.debug("DatabaseConnector.close() called but connection was already None")
        except Exception as e:
            logging.error(f"❌ Unexpected error in DatabaseConnector.close(): {e}")

    def __enter__(self):
        if not self.connection:
            connected = self.connect()
            if not connected:
                raise RuntimeError("Could not connect to DB in context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _cursor(self):
        if not self.connection:
            raise RuntimeError("Attempted to get cursor on closed connection")
        return self.connection.cursor()

    def fetch_accttservicemaster(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            query = """
                SELECT slno, type, code, name
                FROM dba.acc_tt_servicemaster
                WHERE UPPER(TRIM(type)) = 'AREA'
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return rows
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_tt_servicemaster: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_users(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            query = f"SELECT id, pass, role, accountcode FROM {self.config.table_name_users}"
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching users: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_misel(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            query = f"SELECT firm_name, address, phones, mobile, address1, address2, address3, pagers, tinno FROM {self.config.table_name_misel}"
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching misel: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_acc_master(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            query = """
                SELECT 
                    acc_master.code,
                    acc_master.name,
                    acc_master.super_code,
                    acc_master.opening_balance,
                    acc_master.debit,
                    acc_master.credit,
                    acc_master.place,
                    acc_master.phone2,
                    acc_departments.department AS openingdepartment,
                    COALESCE(acc_tt_servicemaster.name, 'No Area') AS area
                FROM acc_master
                LEFT JOIN acc_departments 
                    ON acc_master.openingdepartment = acc_departments.department_id
                LEFT JOIN acc_tt_servicemaster
                    ON acc_master.area = acc_tt_servicemaster.code
                WHERE acc_master.super_code IN ('DEBTO', 'SUNCR', 'CASH', 'BANK');
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            logging.info(f"📊 Fetched {len(results)} acc_master records")
            
            super_code_counts = {}
            for r in results:
                sc = r.get('super_code', 'None')
                super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
            
            logging.info(f"📈 Records by super_code: {super_code_counts}")
            
            area_count = sum(1 for r in results if r.get('area') and str(r['area']).strip())
            logging.info(f"📍 Records with non-empty area field: {area_count}")
            
            return results
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_master: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_acc_ledgers(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo('Asia/Kolkata')
                today = datetime.now(tz).date()
            except Exception:
                try:
                    from pytz import timezone
                    tz = timezone('Asia/Kolkata')
                    today = datetime.now(tz).date()
                except Exception:
                    today = datetime.utcnow().date()
            
            from datetime import timedelta
            fifteen_days_ago = today - timedelta(days=15)
            
            logging.info(f"📅 Fetching acc_ledgers from {fifteen_days_ago.isoformat()} to {today.isoformat()}")
            
            cursor = self._cursor()

            logging.info("Checking acc_ledgers table structure...")
            cursor.execute("SELECT TOP 1 * FROM acc_ledgers")
            logging.info(f"acc_ledgers columns: {[col[0] for col in cursor.description]}")
            cursor.fetchall()

            logging.info("🧪 Debug: Sampling voucher_no values from acc_ledgers...")
            cursor.execute("SELECT TOP 10 code, voucher_no FROM acc_ledgers WHERE voucher_no IS NOT NULL")
            for row in cursor.fetchall():
                logging.info(f"🔎 Sample - code: [{row[0]}], voucher_no: [{row[1]}] (type: {type(row[1])})")

            query = """
                SELECT
                    l.code,
                    l.particulars,
                    l.debit,
                    l.credit,
                    l.entry_mode,
                    l."date" AS entry_date,
                    l.voucher_no,
                    l.narration,
                    m.super_code
                FROM acc_ledgers l
                INNER JOIN acc_master m ON TRIM(l.code) = TRIM(m.code)
                WHERE TRIM(m.super_code) IN ('DEBTO', 'SUNCR', 'CASH', 'BANK')
                AND (
                    (TRIM(m.super_code) IN ('CASH', 'BANK') AND l."date" >= ?)
                    OR
                    (TRIM(m.super_code) IN ('DEBTO', 'SUNCR'))
                )
            """

            logging.info("Executing acc_ledgers query with super_code and date filter...")
            logging.info(f"📍 Date filter: CASH/BANK records >= {fifteen_days_ago.isoformat()}")
            cursor.execute(query, (fifteen_days_ago,))
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            voucher_no_stats = {
                'null': 0,
                'not_null': 0,
                'sample_values': []
            }
            for r in result[:10]:
                if r.get('voucher_no') is None:
                    voucher_no_stats['null'] += 1
                else:
                    voucher_no_stats['not_null'] += 1
                    voucher_no_stats['sample_values'].append(r.get('voucher_no'))
            
            logging.info(f"📊 Voucher_no stats - NULL: {voucher_no_stats['null']}, NOT NULL: {voucher_no_stats['not_null']}")
            logging.info(f"📊 Sample voucher_no values: {voucher_no_stats['sample_values']}")
            
            super_code_counts = {}
            for r in result:
                sc = r.get('super_code', 'None')
                super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
            
            cash_bank_records = [r for r in result if r.get('super_code') in ('CASH', 'BANK')]
            if cash_bank_records:
                dates_with_data = [r.get('entry_date') for r in cash_bank_records if r.get('entry_date')]
                if dates_with_data:
                    min_date = min(dates_with_data)
                    max_date = max(dates_with_data)
                    logging.info(f"📅 CASH/BANK date range: {min_date} to {max_date}")
            
            logging.info(f"✅ Query succeeded! Returned {len(result)} records")
            logging.info(f"📈 Records by super_code: {super_code_counts}")
            
            return result

        except Exception as e:
            logging.error(f"❌ Critical error in fetch_acc_ledgers: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_acc_invmast(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            queries_to_try = [
                """
                SELECT
                    inv.modeofpayment,
                    inv.customerid,
                    inv.invdate,
                    inv.nettotal,
                    inv.paid,
                    inv.type || '-' || inv.billno AS bill_ref
                FROM DBA.acc_invmast AS inv
                INNER JOIN DBA.acc_master AS cust
                    ON inv.customerid = cust.code
                WHERE cust.super_code = 'DEBTO'
                AND inv.paid < inv.nettotal
                AND inv.modeofpayment = 'C'
                """,
                
                """
                SELECT
                    inv.modeofpayment,
                    inv.customerid,
                    inv.invdate,
                    inv.nettotal,
                    inv.paid,
                    CONCAT(inv.type, '-', inv.billno) AS bill_ref
                FROM acc_invmast AS inv
                INNER JOIN acc_master AS cust
                    ON inv.customerid = cust.code
                WHERE cust.super_code = 'DEBTO'
                AND inv.paid < inv.nettotal
                AND inv.modeofpayment = 'C'
                """,
            ]
            
            for i, query in enumerate(queries_to_try, 1):
                try:
                    logging.info(f"Trying acc_invmast query variation {i}...")
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    logging.info(f"✅ acc_invmast query variation {i} succeeded! Returned {len(result)} records")
                    return result
                except Exception as query_e:
                    logging.error(f"❌ acc_invmast query variation {i} failed: {query_e}")
                    continue
            
            logging.error("❌ All acc_invmast query variations failed. Returning empty list.")
            return []
            
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_invmast: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_cashandbankaccmaster(self) -> Optional[List[Dict[str, Any]]]:
        cursor = None
        try:
            cursor = self._cursor()
            query = """
                SELECT code, name, super_code, opening_balance, opening_date, debit, credit
                FROM acc_master
                WHERE super_code IN ('CASH', 'BANK')
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching cashandbankaccmaster: {e}")
            logging.error(traceback.format_exc())
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_sales_today(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch sales records from acc_invmast where billno > 0 AND invdate == today's date (Asia/Kolkata)"""
        cursor = None
        try:
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo('Asia/Kolkata')
                today = datetime.now(tz).date()
            except Exception:
                try:
                    from pytz import timezone
                    tz = timezone('Asia/Kolkata')
                    today = datetime.now(tz).date()
                except Exception:
                    today = datetime.utcnow().date()

            cursor = self._cursor()
            query = """
                SELECT 
                    nettotal,
                    billno,
                    type,
                    userid,
                    invdate,
                    customername
                FROM acc_invmast
                WHERE billno > 0
                AND invdate = ?
                ORDER BY invdate DESC, billno DESC
            """
            logging.info(f"Executing sales_today query for date={today.isoformat()}...")
            cursor.execute(query, (today,))
            columns = [column[0] for column in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logging.info(f"✅ Fetched {len(result)} sales_today records for {today.isoformat()}")
            return result
        except Exception as e:
            logging.error(f"❌ Failed fetching sales_today: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_purchase_today(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch purchase records from acc_purchasemaster where billno > 0 AND date == today's date (Asia/Kolkata)"""
        cursor = None
        try:
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo('Asia/Kolkata')
                today = datetime.now(tz).date()
            except Exception:
                try:
                    from pytz import timezone
                    tz = timezone('Asia/Kolkata')
                    today = datetime.now(tz).date()
                except Exception:
                    today = datetime.utcnow().date()

            cursor = self._cursor()
            query = """
                SELECT 
                    net,
                    billno,
                    pbillno,
                    "date",
                    total,
                    suppliername
                FROM acc_purchasemaster
                WHERE billno > 0
                AND "date" = ?
                ORDER BY "date" DESC, billno DESC
            """
            logging.info(f"Executing purchase_today query for date={today.isoformat()}...")
            cursor.execute(query, (today,))
            columns = [column[0] for column in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logging.info(f"✅ Fetched {len(result)} purchase_today records for {today.isoformat()}")
            return result
        except Exception as e:
            logging.error(f"❌ Failed fetching purchase_today: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_sales_daywise(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch sales summary by date for last 8 days from acc_invmast"""
        cursor = None
        try:
            cursor = self._cursor()
            
            query = """
                SELECT  
                    invdate AS "date", 
                    COUNT(*) AS total_bills, 
                    SUM(nettotal) AS total_amount 
                FROM acc_invmast 
                WHERE billno > 0 
                AND invdate BETWEEN DATEADD(DAY, -7, CURRENT DATE) AND CURRENT DATE 
                GROUP BY invdate 
                ORDER BY invdate DESC
            """
            
            logging.info("📅 Executing sales_daywise query (last 8 days)...")
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            logging.info(f"✅ Fetched {len(result)} sales_daywise records")
            return result
            
        except Exception as e:
            logging.error(f"❌ Failed fetching sales_daywise: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_sales_monthwise(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch sales summary by month for current year from acc_invmast"""
        cursor = None
        try:
            cursor = self._cursor()
            
            query = """
                SELECT  
                    MONTH(invdate) AS month_number, 
                    YEAR(invdate) AS year, 
                    COUNT(*) AS total_bills, 
                    SUM(nettotal) AS total_amount 
                FROM acc_invmast 
                WHERE billno > 0 
                AND YEAR(invdate) = YEAR(CURRENT DATE) 
                GROUP BY YEAR(invdate), MONTH(invdate) 
                ORDER BY MONTH(invdate)
            """
            
            logging.info("📅 Executing sales_monthwise query (current year)...")
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Add month names to the results
            month_names = [
                'January', 'February', 'March', 'April', 'May', 'June',
                'July', 'August', 'September', 'October', 'November', 'December'
            ]
            
            for row in result:
                month_num = row.get('month_number', 1)
                year = row.get('year', datetime.now().year)
                if 1 <= month_num <= 12:
                    row['month_name'] = f"{month_names[month_num - 1]} {year}"
                else:
                    row['month_name'] = f"Month {month_num} {year}"
            
            logging.info(f"✅ Fetched {len(result)} sales_monthwise records")
            return result
            
        except Exception as e:
            logging.error(f"❌ Failed fetching sales_monthwise: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass


class WebAPIClient:
    # API Endpoints defined as class constants
    ENDPOINT_USERS = "/upload-users/"
    ENDPOINT_MISEL = "/upload-misel/"
    ENDPOINT_ACC_MASTER = "/upload-acc-master/"
    ENDPOINT_ACC_LEDGERS = "/upload-acc-ledgers/"
    ENDPOINT_ACC_INVMAST = "/upload-acc-invmast/"
    ENDPOINT_CASH_BANK = "/upload-cashandbankaccmaster/"
    ENDPOINT_ACC_TT_SERVICE = "/upload-accttservicemaster/"
    ENDPOINT_SALES_TODAY = "/upload-sales-today/"
    ENDPOINT_PURCHASE_TODAY = "/upload-purchase-today/"
    ENDPOINT_SALES_DAYWISE = "/upload-sales-daywise/"
    ENDPOINT_SALES_MONTHWISE = "/upload-sales-monthwise/"

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({'Content-Type': 'application/json'})
        return session

    def upload_accttservicemaster(self, rows: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.ENDPOINT_ACC_TT_SERVICE}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=rows, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ acc_tt_servicemaster uploaded successfully")
                return True
            else:
                logging.error(f"❌ acc_tt_servicemaster upload failed: {res.status_code} – {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_accttservicemaster: {e}")
            return False

    def upload_users(self, users: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.ENDPOINT_USERS}?client_id={self.config.client_id}"
        try:
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

    def upload_misel(self, misel: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.ENDPOINT_MISEL}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=misel, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Misel uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_misel: {e}")
            return False

    def upload_acc_master(self, acc_master: List[Dict[str, Any]]) -> bool:
        if not acc_master:
            logging.warning("No acc_master data to upload")
            return True
        
        if len(acc_master) > 1000:
            logging.info(f"📦 Large dataset detected ({len(acc_master)} records). Using batch upload...")
            return self._upload_in_batches_with_clear('acc_master', acc_master, batch_size=200)
        
        url = f"{self.config.api_base_url}{self.ENDPOINT_ACC_MASTER}?client_id={self.config.client_id}&force_clear=true"
        try:
            logging.info("🧹 Clearing existing acc_master data...")
            clear_res = self.session.post(url, json=[], timeout=60)
            
            if clear_res.status_code not in [200, 201]:
                logging.error(f"❌ Failed to clear existing acc_master data: {clear_res.status_code} - {clear_res.text}")
                return False
            
            logging.info(f"📤 Uploading {len(acc_master)} acc_master records...")
            res = self.session.post(url, json=acc_master, timeout=120)
            
            if res.status_code in [200, 201]:
                logging.info("✅ Acc_Master uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Exception in upload_acc_master: {e}")
            return False

    def _upload_in_batches_with_clear(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 500) -> bool:
        if not data:
            return True
        
        endpoint_map = {
            'acc_master': self.ENDPOINT_ACC_MASTER,
            'acc_ledgers': self.ENDPOINT_ACC_LEDGERS,
            'acc_invmast': self.ENDPOINT_ACC_INVMAST
        }
        
        endpoint = endpoint_map.get(table_name, self.ENDPOINT_ACC_MASTER)
        total_records = len(data)
        url = f"{self.config.api_base_url}{endpoint}?client_id={self.config.client_id}"
        
        try:
            logging.info(f"🧹 Clearing existing {table_name} data...")
            clear_url = f"{url}&force_clear=true"
            res = self.session.post(clear_url, json=[], timeout=60)
            if res.status_code not in [200, 201]:
                logging.error(f"❌ Failed to clear existing data: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception clearing data: {e}")
            return False
        
        success_count = 0
        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            try:
                logging.info(f"📤 Uploading {table_name} batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                if table_name == 'acc_master':
                    timeout = min(240, max(120, int(len(batch) * 0.5)))
                else:
                    timeout = min(180, max(60, int(len(batch) // 5)))
                
                batch_url = f"{url}&append=true" if i > 0 else url
                
                res = self.session.post(batch_url, json=batch, timeout=timeout)
                
                if res.status_code in [200, 201]:
                    success_count += len(batch)
                    logging.info(f"✅ Batch {batch_num}/{total_batches} uploaded successfully")
                else:
                    logging.error(f"❌ Batch {batch_num} failed: {res.status_code} - {res.text}")
                    return False
                    
            except Exception as e:
                logging.error(f"❌ Exception in batch {batch_num}: {e}")
                return False
        
        logging.info(f"✅ {table_name.title()} uploaded successfully ({success_count}/{total_records} records)")
        return True

    def _upload_in_batches(self, endpoint_key: str, data: List[Dict[str, Any]], batch_size: int = None) -> bool:
        if not data:
            return True
        
        if batch_size is None:
            batch_size = self.config.batch_size
        
        endpoint_map = {
            'acc_ledgers': self.ENDPOINT_ACC_LEDGERS,
            'acc_invmast': self.ENDPOINT_ACC_INVMAST
        }
        
        endpoint = endpoint_map.get(endpoint_key, f"/upload-{endpoint_key}/")
        total_records = len(data)
        url = f"{self.config.api_base_url}{endpoint}?client_id={self.config.client_id}"
        
        if total_records > batch_size:
            try:
                logging.info(f"🧹 Clearing existing {endpoint_key} data...")
                res = self.session.post(url, json=[], timeout=60)
                if res.status_code not in [200, 201]:
                    logging.error(f"❌ Failed to clear existing data: {res.status_code} - {res.text}")
            except Exception as e:
                logging.error(f"❌ Exception clearing data: {e}")
        
        success_count = 0
        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            try:
                logging.info(f"📤 Uploading {endpoint_key} batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                timeout = min(180, max(60, int(len(batch) // 5)))
                
                batch_url = url
                if total_records > batch_size and i > 0:
                    batch_url = f"{url}&append=true"
                
                res = self.session.post(batch_url, json=batch, timeout=timeout)
                
                if res.status_code in [200, 201]:
                    success_count += len(batch)
                    logging.info(f"✅ Batch {batch_num}/{total_batches} uploaded successfully")
                else:
                    logging.error(f"❌ Batch {batch_num} failed: {res.status_code} - {res.text}")
                    return False
                    
            except Exception as e:
                logging.error(f"❌ Exception in batch {batch_num}: {e}")
                return False
        
        logging.info(f"✅ {endpoint_key.title()} uploaded successfully ({success_count}/{total_records} records)")
        return True

    def upload_acc_ledgers(self, acc_ledgers: List[Dict[str, Any]]) -> bool:
        return self._upload_in_batches('acc_ledgers', acc_ledgers, self.config.large_table_batch_size)

    def upload_acc_invmast(self, acc_invmast: List[Dict[str, Any]]) -> bool:
        if not acc_invmast:
            logging.warning("No acc_invmast data to upload")
            return True
        
        if len(acc_invmast) > 1000:
            logging.info(f"📦 Large dataset detected ({len(acc_invmast)} records). Using batch upload...") 
            return self._upload_in_batches_with_clear('acc_invmast', acc_invmast, batch_size=500)
        
        url = f"{self.config.api_base_url}{self.ENDPOINT_ACC_INVMAST}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=acc_invmast, timeout=120)
            if res.status_code in [200, 201]:
                logging.info("✅ AccInvmast uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_acc_invmast: {e}")
            return False

    def upload_cashandbankaccmaster(self, cashandbankaccmaster: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.ENDPOINT_CASH_BANK}?client_id={self.config.client_id}"
        try:
            logging.info("🧹 Clearing existing cashandbankaccmaster data...")
            clear_url = f"{url}&force_clear=true"
            clear_res = self.session.post(clear_url, json=[], timeout=60)
            
            if clear_res.status_code not in [200, 201]:
                logging.error(f"❌ Failed to clear existing data: {clear_res.status_code} - {clear_res.text}")
            
            res = self.session.post(url, json=cashandbankaccmaster, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ CashAndBankAccMaster uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_cashandbankaccmaster: {e}")
            return False

    def upload_sales_today(self, sales_today: List[Dict[str, Any]]) -> bool:
        """Upload sales_today records"""
        url = f"{self.config.api_base_url}{self.ENDPOINT_SALES_TODAY}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=sales_today, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Sales_Today uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_sales_today: {e}")
            return False

    def upload_purchase_today(self, purchase_today: List[Dict[str, Any]]) -> bool:
        """Upload purchase_today records"""
        url = f"{self.config.api_base_url}{self.ENDPOINT_PURCHASE_TODAY}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=purchase_today, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Purchase_Today uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_purchase_today: {e}")
            return False

    def upload_sales_daywise(self, sales_daywise: List[Dict[str, Any]]) -> bool:
        """Upload sales_daywise records"""
        url = f"{self.config.api_base_url}{self.ENDPOINT_SALES_DAYWISE}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=sales_daywise, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Sales_Daywise uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_sales_daywise: {e}")
            return False

    def upload_sales_monthwise(self, sales_monthwise: List[Dict[str, Any]]) -> bool:
        """Upload sales_monthwise records"""
        url = f"{self.config.api_base_url}{self.ENDPOINT_SALES_MONTHWISE}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=sales_monthwise, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Sales_Monthwise uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_sales_monthwise: {e}")
            return False


class SyncTool:
    def __init__(self):
        self.config: Optional[DatabaseConfig] = None
        self.db_connector: Optional[DatabaseConnector] = None
        self.api_client: Optional[WebAPIClient] = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
        logging.info("=== SQL Anywhere Sync Tool Started ===")

    def _setup_logging(self):
        level = logging.INFO
        if self.config and self.config.log_level:
            level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.getLogger().setLevel(level)

    def initialize(self) -> bool:
        try:
            self.config = DatabaseConfig()
            self._setup_logging()
            self.db_connector = DatabaseConnector(self.config)
            self.api_client = WebAPIClient(self.config)
            return True
        except Exception as e:
            logging.error(f"Initialization failed: {e}")
            logging.error(traceback.format_exc())
            return False

    def validate_accttservicemaster_data(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for r in rows:
            try:
                valid.append({
                    'slno': int(r['slno']),
                    'type': str(r['type']) if r.get('type') else None,
                    'code': str(r['code']) if r.get('code') else None,
                    'name': str(r['name']) if r.get('name') else None
                })
            except (ValueError, TypeError):
                continue
        return valid

    def validate_user_data(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid_users = []
        for i, user in enumerate(users):
            if not user.get('id') or not user.get('pass'):
                continue
            valid_users.append({
                'id': str(user['id']).strip(),
                'pass': str(user['pass']).strip(),
                'role': user.get('role', '').strip() if user.get('role') else None,
                'accountcode': user.get('accountcode', '').strip() if user.get('accountcode') else None
            })
        return valid_users

    def validate_misel_data(self, misel: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(misel):
            if not m.get('firm_name'):
                continue
            valid.append({
                'firm_name': m['firm_name'],
                'address': m.get('address', ''),
                'phones': m.get('phones', ''),
                'mobile': m.get('mobile', ''),
                'address1': m.get('address1', ''),
                'address2': m.get('address2', ''),
                'address3': m.get('address3', ''),
                'pagers': m.get('pagers', ''),
                'tinno': m.get('tinno', ''),
            })
        return valid

    def validate_acc_master_data(self, acc_master: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(acc_master):
            if not m.get('code'):
                continue
            
            area_value = m.get('area', '')
            if area_value and area_value != 'No Area':
                area_clean = str(area_value).strip()
            else:
                area_clean = None
            
            super_code = str(m.get('super_code', '')).strip() if m.get('super_code') else None
            
            validated_record = {
                'code': str(m['code']).strip(),
                'name': str(m.get('name', '')).strip() if m.get('name') else '',
                'super_code': super_code,
                'opening_balance': float(m['opening_balance']) if m.get('opening_balance') is not None else None,
                'debit': float(m['debit']) if m.get('debit') is not None else None,
                'credit': float(m['credit']) if m.get('credit') is not None else None,
                'place': str(m.get('place', '')).strip() if m.get('place') else '',
                'phone2': str(m.get('phone2', '')).strip() if m.get('phone2') else '',
                'openingdepartment': str(m.get('openingdepartment', '')).strip() if m.get('openingdepartment') else '',
                'area': area_clean
            }
            
            valid.append(validated_record)
            
        area_count = sum(1 for r in valid if r.get('area'))
        super_code_counts = {}
        for r in valid:
            sc = r.get('super_code', 'None')
            super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
        
        logging.info(f"📊 After validation - Records with area data: {area_count}/{len(valid)}")
        logging.info(f"📊 After validation - Records by super_code: {super_code_counts}")
        
        return valid

    def validate_acc_ledgers_data(self, acc_ledgers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        voucher_errors = []
        
        for i, l in enumerate(acc_ledgers):
            if not l.get('code'):
                continue
            
            entry_date = None
            if l.get('entry_date'):
                try:
                    if hasattr(l['entry_date'], 'strftime'):
                        entry_date = l['entry_date'].strftime('%Y-%m-%d')
                    elif isinstance(l['entry_date'], str):
                        from datetime import datetime
                        try:
                            parsed_date = datetime.strptime(l['entry_date'], '%Y-%m-%d')
                            entry_date = parsed_date.strftime('%Y-%m-%d')
                        except ValueError:
                            for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                                try:
                                    parsed_date = datetime.strptime(l['entry_date'], fmt)
                                    entry_date = parsed_date.strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    continue
                    else:
                        entry_date = str(l['entry_date'])
                except Exception as date_e:
                    logging.warning(f"Could not parse date {l['entry_date']}: {date_e}")
                    entry_date = None
            
            voucher_no = None
            raw_voucher = l.get('voucher_no')
            
            if raw_voucher is not None:
                try:
                    if isinstance(raw_voucher, int):
                        voucher_no = raw_voucher
                    elif isinstance(raw_voucher, float):
                        voucher_no = int(raw_voucher)
                    elif isinstance(raw_voucher, str):
                        cleaned = raw_voucher.strip()
                        if cleaned:
                            voucher_no = int(float(cleaned))
                    else:
                        voucher_no = int(raw_voucher)
                        
                except (ValueError, TypeError) as voucher_e:
                    voucher_errors.append({
                        'index': i,
                        'code': l.get('code'),
                        'raw_value': raw_voucher,
                        'type': type(raw_voucher).__name__,
                        'error': str(voucher_e)
                    })
                    voucher_no = None
            
            debit = None
            credit = None
            try:
                if l.get('debit') is not None:
                    debit = float(l['debit'])
            except (ValueError, TypeError):
                debit = None
                
            try:
                if l.get('credit') is not None:
                    credit = float(l['credit'])
            except (ValueError, TypeError):
                credit = None
            
            super_code = str(l.get('super_code', '')).strip() if l.get('super_code') else None
            
            valid.append({
                'code': str(l['code']).strip(),
                'particulars': l.get('particulars', ''),
                'debit': debit,
                'credit': credit,
                'entry_mode': l.get('entry_mode', ''),
                'entry_date': entry_date,
                'voucher_no': voucher_no,
                'narration': l.get('narration', ''),
                'super_code': super_code
            })
        
        voucher_stats = {
            'total_records': len(valid),
            'with_voucher_no': sum(1 for r in valid if r.get('voucher_no') is not None),
            'without_voucher_no': sum(1 for r in valid if r.get('voucher_no') is None),
            'conversion_errors': len(voucher_errors)
        }
        
        logging.info(f"📊 Voucher_no validation stats: {voucher_stats}")
        
        if voucher_errors:
            logging.warning(f"⚠️ Found {len(voucher_errors)} voucher_no conversion errors")
            for err in voucher_errors[:5]:
                logging.warning(f"  - Record {err['index']}, code={err['code']}: "
                            f"value={err['raw_value']} (type={err['type']}), error={err['error']}")
        
        super_code_counts = {}
        for r in valid:
            sc = r.get('super_code', 'None')
            super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
        
        logging.info(f"📊 After validation - Records by super_code: {super_code_counts}")
        
        return valid

    def validate_acc_invmast_data(self, acc_invmast: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, inv in enumerate(acc_invmast):
            invdate = None
            if inv.get('invdate'):
                try:
                    if hasattr(inv['invdate'], 'strftime'):
                        invdate = inv['invdate'].strftime('%Y-%m-%d')
                    else:
                        invdate = str(inv['invdate'])
                except Exception:
                    invdate = None
            
            nettotal = None
            paid = None
            try:
                if inv.get('nettotal') is not None:
                    nettotal = float(inv['nettotal'])
            except (ValueError, TypeError):
                nettotal = None
                
            try:
                if inv.get('paid') is not None:
                    paid = float(inv['paid'])
            except (ValueError, TypeError):
                paid = None
            
            valid.append({
                'modeofpayment': inv.get('modeofpayment', ''),
                'customerid': inv.get('customerid', ''),
                'invdate': invdate,
                'nettotal': nettotal,
                'paid': paid,
                'bill_ref': inv.get('bill_ref', '')
            })
        return valid

    def validate_cashandbankaccmaster_data(self, cashandbankaccmaster: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(cashandbankaccmaster):
            if not m.get('code'):
                continue
            valid.append({
                'code': str(m['code']).strip(),
                'name': m.get('name', ''),
                'super_code': m.get('super_code', ''),
                'opening_balance': float(m['opening_balance']) if m.get('opening_balance') else None,
                'opening_date': m['opening_date'].strftime('%Y-%m-%d') if m.get('opening_date') else None,
                'debit': float(m['debit']) if m.get('debit') else None,
                'credit': float(m['credit']) if m.get('credit') else None,
                'client_id': self.config.client_id
            })
        return valid

    def validate_sales_today_data(self, sales_today: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate sales_today data"""
        valid = []
        for i, s in enumerate(sales_today):
            invdate = None
            if s.get('invdate'):
                try:
                    if hasattr(s['invdate'], 'strftime'):
                        invdate = s['invdate'].strftime('%Y-%m-%d')
                    else:
                        invdate = str(s['invdate'])
                except Exception:
                    invdate = None
            
            nettotal = None
            try:
                if s.get('nettotal') is not None:
                    nettotal = float(s['nettotal'])
            except (ValueError, TypeError):
                nettotal = None
            
            valid.append({
                'nettotal': nettotal,
                'billno': int(s['billno']) if s.get('billno') else None,
                'type': s.get('type', ''),
                'userid': s.get('userid', ''),
                'invdate': invdate,
                'customername': s.get('customername', '')
            })
        return valid

    def validate_purchase_today_data(self, purchase_today: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate purchase_today data"""
        valid = []
        for i, p in enumerate(purchase_today):
            date = None
            if p.get('date'):
                try:
                    if hasattr(p['date'], 'strftime'):
                        date = p['date'].strftime('%Y-%m-%d')
                    else:
                        date = str(p['date'])
                except Exception:
                    date = None
            
            net = None
            total = None
            try:
                if p.get('net') is not None:
                    net = float(p['net'])
            except (ValueError, TypeError):
                net = None
                
            try:
                if p.get('total') is not None:
                    total = float(p['total'])
            except (ValueError, TypeError):
                total = None
            
            valid.append({
                'net': net,
                'billno': int(p['billno']) if p.get('billno') else None,
                'pbillno': int(p['pbillno']) if p.get('pbillno') else None,
                'date': date,
                'total': total,
                'suppliername': p.get('suppliername', '')
            })
        return valid

    def validate_sales_daywise_data(self, sales_daywise: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate sales_daywise data"""
        valid = []
        for i, s in enumerate(sales_daywise):
            date = None
            if s.get('date'):
                try:
                    if hasattr(s['date'], 'strftime'):
                        date = s['date'].strftime('%Y-%m-%d')
                    else:
                        date = str(s['date'])
                except Exception:
                    date = None
            
            total_bills = 0
            total_amount = 0
            try:
                if s.get('total_bills') is not None:
                    total_bills = int(s['total_bills'])
            except (ValueError, TypeError):
                total_bills = 0
                
            try:
                if s.get('total_amount') is not None:
                    total_amount = float(s['total_amount'])
            except (ValueError, TypeError):
                total_amount = 0
            
            valid.append({
                'date': date,
                'total_bills': total_bills,
                'total_amount': total_amount
            })
        return valid

    def validate_sales_monthwise_data(self, sales_monthwise: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate sales_monthwise data"""
        valid = []
        for i, s in enumerate(sales_monthwise):
            month_number = 1
            year = datetime.now().year
            total_bills = 0
            total_amount = 0
            
            try:
                if s.get('month_number') is not None:
                    month_number = int(s['month_number'])
            except (ValueError, TypeError):
                month_number = 1
            
            try:
                if s.get('year') is not None:
                    year = int(s['year'])
            except (ValueError, TypeError):
                year = datetime.now().year
                
            try:
                if s.get('total_bills') is not None:
                    total_bills = int(s['total_bills'])
            except (ValueError, TypeError):
                total_bills = 0
                
            try:
                if s.get('total_amount') is not None:
                    total_amount = float(s['total_amount'])
            except (ValueError, TypeError):
                total_amount = 0
            
            valid.append({
                'month_name': s.get('month_name', f'Month {month_number}'),
                'month_number': month_number,
                'year': year,
                'total_bills': total_bills,
                'total_amount': total_amount
            })
        return valid

    def run(self) -> bool:
        print("🔄 Starting SQL Anywhere to Web API sync...")
        if not self.initialize():
            return False
        if not self.db_connector.connect():
            return False

        # Sync Users
        users = self.db_connector.fetch_users()
        if users:
            print(f"📊 Found {len(users)} users")
            valid_users = self.validate_user_data(users)
            if valid_users:
                self.api_client.upload_users(valid_users)
            else:
                print("❌ No valid user data")

        # Sync Misel
        misel = self.db_connector.fetch_misel()
        if misel:
            print(f"📊 Found {len(misel)} misel entries")
            valid_misel = self.validate_misel_data(misel)
            if valid_misel:
                self.api_client.upload_misel(valid_misel)
            else:
                print("❌ No valid misel data")

        # Sync AccMaster
        acc_master = self.db_connector.fetch_acc_master()
        if acc_master:
            print(f"📊 Found {len(acc_master)} acc_master entries")
            valid_acc_master = self.validate_acc_master_data(acc_master)
            if valid_acc_master:
                super_code_counts = {}
                for r in valid_acc_master:
                    sc = r.get('super_code', 'None')
                    super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
                
                print(f"📈 Records by super_code: {super_code_counts}")
                
                area_records = [r for r in valid_acc_master if r.get('area')]
                print(f"📊 Records with area data: {len(area_records)}/{len(valid_acc_master)}")
                
                if area_records:
                    sample_areas = [r['area'] for r in area_records[:5]]
                    print(f"📍 Sample area values: {sample_areas}")
                
                if not self.api_client.upload_acc_master(valid_acc_master):
                    print("❌ CRITICAL: acc_master upload failed! Stopping sync.")
                    try:
                        self.db_connector.close()
                    except Exception:
                        pass
                    return False
            else:
                print("❌ No valid acc_master data")

        # Sync AccLedgers
        acc_ledgers = self.db_connector.fetch_acc_ledgers()
        if acc_ledgers is not None:
            if acc_ledgers:
                print(f"📊 Found {len(acc_ledgers)} acc_ledgers entries")
                
                super_code_counts = {}
                for r in acc_ledgers:
                    sc = r.get('super_code', 'None')
                    super_code_counts[sc] = super_code_counts.get(sc, 0) + 1
                print(f"📈 Ledgers by super_code: {super_code_counts}")
                
                valid_acc_ledgers = self.validate_acc_ledgers_data(acc_ledgers)
                if valid_acc_ledgers:
                    self.api_client.upload_acc_ledgers(valid_acc_ledgers)
                else:
                    print("❌ No valid acc_ledgers data")
            else:
                print("📊 Found 0 acc_ledgers entries")
        else:
            print("❌ Failed to fetch acc_ledgers data")

        # Sync AccInvmast
        acc_invmast = self.db_connector.fetch_acc_invmast()
        if acc_invmast is not None:
            if acc_invmast:
                print(f"📊 Found {len(acc_invmast)} acc_invmast entries")
                valid_acc_invmast = self.validate_acc_invmast_data(acc_invmast)
                if valid_acc_invmast:
                    self.api_client.upload_acc_invmast(valid_acc_invmast)
                else:
                    print("❌ No valid acc_invmast data")
            else:
                print("📊 Found 0 acc_invmast entries")
        else:
            print("❌ Failed to fetch acc_invmast data")

        # Sync CashAndBankAccMaster
        cashandbankaccmaster = self.db_connector.fetch_cashandbankaccmaster()
        if cashandbankaccmaster:
            print(f"📊 Found {len(cashandbankaccmaster)} cashandbankaccmaster entries")
            valid_cashandbankaccmaster = self.validate_cashandbankaccmaster_data(cashandbankaccmaster)
            if valid_cashandbankaccmaster:
                self.api_client.upload_cashandbankaccmaster(valid_cashandbankaccmaster)
            else:
                print("❌ No valid cashandbankaccmaster data")

        # Sync acc_tt_servicemaster
        acctt = self.db_connector.fetch_accttservicemaster()
        if acctt:
            print(f"📊 Found {len(acctt)} acc_tt_servicemaster rows")
            valid = self.validate_accttservicemaster_data(acctt)
            if valid:
                self.api_client.upload_accttservicemaster(valid)
            else:
                print("❌ No valid acc_tt_servicemaster data")

        # Sync Sales Today
        sales_today = self.db_connector.fetch_sales_today()
        if sales_today is not None:
            if sales_today:
                print(f"📊 Found {len(sales_today)} sales_today entries")
                valid_sales_today = self.validate_sales_today_data(sales_today)
                if valid_sales_today:
                    self.api_client.upload_sales_today(valid_sales_today)
                else:
                    print("❌ No valid sales_today data")
            else:
                print("📊 Found 0 sales_today entries")
        else:
            print("❌ Failed to fetch sales_today data")

        # Sync Purchase Today
        purchase_today = self.db_connector.fetch_purchase_today()
        if purchase_today is not None:
            if purchase_today:
                print(f"📊 Found {len(purchase_today)} purchase_today entries")
                valid_purchase_today = self.validate_purchase_today_data(purchase_today)
                if valid_purchase_today:
                    self.api_client.upload_purchase_today(valid_purchase_today)
                else:
                    print("❌ No valid purchase_today data")
            else:
                print("📊 Found 0 purchase_today entries")
        else:
            print("❌ Failed to fetch purchase_today data")

        # Sync Sales Daywise
        sales_daywise = self.db_connector.fetch_sales_daywise()
        if sales_daywise is not None:
            if sales_daywise:
                print(f"📊 Found {len(sales_daywise)} sales_daywise entries")
                valid_sales_daywise = self.validate_sales_daywise_data(sales_daywise)
                if valid_sales_daywise:
                    self.api_client.upload_sales_daywise(valid_sales_daywise)
                else:
                    print("❌ No valid sales_daywise data")
            else:
                print("📊 Found 0 sales_daywise entries")
        else:
            print("❌ Failed to fetch sales_daywise data")

        # Sync Sales Monthwise
        sales_monthwise = self.db_connector.fetch_sales_monthwise()
        if sales_monthwise is not None:
            if sales_monthwise:
                print(f"📊 Found {len(sales_monthwise)} sales_monthwise entries")
                valid_sales_monthwise = self.validate_sales_monthwise_data(sales_monthwise)
                if valid_sales_monthwise:
                    self.api_client.upload_sales_monthwise(valid_sales_monthwise)
                else:
                    print("❌ No valid sales_monthwise data")
            else:
                print("📊 Found 0 sales_monthwise entries")
        else:
            print("❌ Failed to fetch sales_monthwise data")

        # Ensure DB connection closed
        try:
            self.db_connector.close()
        except Exception:
            pass
        return True

    def run_interactive(self):
        print("=" * 60)
        print("    SQL Anywhere to Web API Sync Tool")
        print("=" * 60)
        print()
        try:
            if self.run():
                print("\n✅ Sync completed successfully!")
            else:
                print("\n❌ Sync failed!")
        except Exception as e:
            print(f"❌ Critical error: {e}")
        print("\nPress Enter to exit...")
        input()


def main():
    sync_tool = SyncTool()
    sync_tool.run_interactive()
    

if __name__ == "__main__":
    main()