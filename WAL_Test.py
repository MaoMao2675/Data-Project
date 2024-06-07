import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastparquet
import threading


# Build Write Ahead Log Function
class WriteAheadLog:
    def __init__(self, wal_dir, max_wal_entries=100, max_wal_files=5):
        self.wal_dir = wal_dir
        self.max_wal_entries = max_wal_entries
        self.max_wal_files = max_wal_files
        self.current_wal_file = None
        self.wal_counter = 0
        self.current_file_index = 0
        self._ensure_wal_dir_exists()
        self._initialize_current_wal_file()

    def _ensure_wal_dir_exists(self):
        os.makedirs(self.wal_dir, exist_ok=True)
        print(f"Ensured WAL directory exists: {self.wal_dir}")

    def _initialize_current_wal_file(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        self.current_file_index = len(wal_files)
        self.current_wal_file_path = os.path.join(self.wal_dir, f"{self.current_file_index}.wal")
        self.current_wal_file = open(self.current_wal_file_path, 'a')
        print(f"Initialized WAL file: {self.current_wal_file_path}")

    def _rotate_wal_file(self):
        self.current_wal_file.close()
        self.current_file_index += 1
        self.current_wal_file_path = os.path.join(self.wal_dir, f"{self.current_file_index}.wal")
        self.current_wal_file = open(self.current_wal_file_path, 'a')
        self.wal_counter = 0
        print(f"Rotated WAL file: {self.current_wal_file_path}")
        self._cleanup_old_wal_files()

    def _cleanup_old_wal_files(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        while len(wal_files) > self.max_wal_files:
            oldest_file = wal_files.pop(0)
            os.remove(os.path.join(self.wal_dir, oldest_file))
            print(f"Deleted old WAL file: {oldest_file}")

    def log(self, entry):
        if self.wal_counter >= self.max_wal_entries:
            self._rotate_wal_file()
        self.current_wal_file.write(json.dumps(entry) + '\n')
        self.current_wal_file.flush()
        os.fsync(self.current_wal_file.fileno())
        self.wal_counter += 1
        print(f"Logged entry: {entry}")

    def start_transaction(self):
        self.log({'action': 'start'})

    def end_transaction(self):
        self.log({'action': 'end'})

    def log_insert(self, key, value):
        self.log({'action': 'insert', 'key': key, 'value': value, 'old_value': None})

    def log_update(self, key, old_value, new_value):
        self.log({'action': 'update', 'key': key, 'value': new_value, 'old_value': old_value})

    def log_delete(self, key, old_value):
        self.log({'action': 'delete', 'key': key, 'value': None, 'old_value': old_value})

    def load_wal(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        transactions = []
        for wal_file in wal_files:
            with open(os.path.join(self.wal_dir, wal_file), 'r') as file:
                transaction = []
                for line in file:
                    log_entry = json.loads(line.strip())
                    if log_entry['action'] == 'start':
                        transaction = []
                    elif log_entry['action'] == 'end':
                        transactions.append(transaction)
                    else:
                        transaction.append(log_entry)
        print(f"Loaded WAL transactions: {transactions}")
        return transactions


# Testing the WriteAheadLog class
def test_wal():
    wal_dir = 'test_wal_dir'
    if not os.path.exists(wal_dir):
        os.makedirs(wal_dir)

    # Initialize WriteAheadLog with max_wal_entries of 3 for easier testing
    wal = WriteAheadLog(wal_dir, max_wal_entries=3, max_wal_files=2)

    # Start a transaction
    wal.start_transaction()
    wal.log_insert('key1', 'value1')
    wal.log_update('key1', 'value1', 'value2')
    wal.log_delete('key1', 'value2')
    wal.end_transaction()

    # Start another transaction to test rotation
    wal.start_transaction()
    wal.log_insert('key2', 'value3')
    wal.log_update('key2', 'value3', 'value4')
    wal.end_transaction()

    # Load WAL on startup
    transactions = wal.load_wal()
    for i, transaction in enumerate(transactions):
        print(f"Transaction {i + 1}:")
        for entry in transaction:
            print(entry)

    # Cleanup test directory after the test
    for file in os.listdir(wal_dir):
        os.remove(os.path.join(wal_dir, file))
    os.rmdir(wal_dir)


test_wal()


# Build KVShard Class for single shard

class KVShard:
    def __init__(self, shard_id, wal_dir, data_file, index_file):
        self.shard_id = shard_id
        self.data = {}
        self.index = {}
        self.wal = WriteAheadLog(wal_dir)
        self.data_file = data_file
        self.index_file = index_file
        self.lock = threading.Lock()
        self.load_wal()
        self.load_data()

    def load_wal(self):
        """Load and process WAL files to restore the state."""
        transactions = self.wal.load_wal()
        for transaction in transactions:
            for log_entry in transaction:
                self.apply_change(log_entry, apply_to_data=False)

    def load_data(self):
        """Load data and index from Parquet files."""
        if os.path.exists(self.data_file):
            df = pd.read_parquet(self.data_file)
            self.data = df.set_index('key').to_dict()['value']
        if os.path.exists(self.index_file):
            df = pd.read_parquet(self.index_file)
            self.index = df.set_index('value').groupby(level=0).agg(list).to_dict()['key']

    def apply_change(self, log_entry, apply_to_data=True):
        """Apply a logged change to the data structure and secondary index."""
        key = log_entry['key']
        value = log_entry['value']
        old_value = log_entry['old_value']
        action = log_entry['action']
        if apply_to_data:
            if action == 'insert' or action == 'update':
                self.data[key] = value
                if old_value:
                    self.index[old_value].remove(key)
                    if not self.index[old_value]:
                        del self.index[old_value]
                if value not in self.index:
                    self.index[value] = []
                self.index[value].append(key)
            elif action == 'delete':
                del self.data[key]
                self.index[value].remove(key)
                if not self.index[value]:
                    del self.index[value]

    def save_to_parquet(self):
        """Save the data and index to Parquet files."""
        data_df = pd.DataFrame(list(self.data.items()), columns=['key', 'value'])
        data_df.to_parquet(self.data_file, index=False)
        index_df = pd.DataFrame([(k, v) for k, keys in self.index.items() for v in keys], columns=['value', 'key'])
        index_df.to_parquet(self.index_file, index=False)

    def start_transaction(self):
        """Start a new transaction."""
        self.wal.start_transaction()

    def end_transaction(self):
        """End the current transaction."""
        self.wal.end_transaction()
        self.save_to_parquet()

    def insert(self, key, value):
        """Insert a new key-value pair."""
        with self.lock:
            old_value = self.data.get(key)
            self.wal.log_insert(key, value)
            self.apply_change({'action': 'insert', 'key': key, 'value': value, 'old_value': old_value})

    def update(self, key, value):
        """Update an existing key-value pair."""
        with self.lock:
            old_value = self.data.get(key)
            self.wal.log_update(key, old_value, value)
            self.apply_change({'action': 'update', 'key': key, 'value': value, 'old_value': old_value})

    def delete(self, key):
        """Delete a key-value pair."""
        with self.lock:
            value = self.data.get(key)
            self.wal.log_delete(key, value)
            self.apply_change({'action': 'delete', 'key': key, 'value': value, 'old_value': None})

    def get(self, key):
        """Retrieve a value by key."""
        with self.lock:
            return self.data.get(key)

# 

