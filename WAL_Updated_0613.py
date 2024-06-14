import os
import json
from datetime import datetime


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


# validation with a few transactions
def test_wal():
    wal_dir = 'wal_test'
    wal = WriteAheadLog(wal_dir, max_wal_entries=10, max_wal_files=2)

    # Start a transaction
    wal.start_transaction()
    wal.log_insert('123-45-6789', {'State': 'CA', 'Occupation': 'Engineer'})
    wal.log_update('123-45-6789', {'State': 'CA', 'Occupation': 'Engineer'},
                   {'State': 'CA', 'Occupation': 'Senior Engineer'})
    wal.log_delete('123-45-6789', {'State': 'CA', 'Occupation': 'Senior Engineer'})
    wal.end_transaction()

    # Start another transaction to test rotation
    wal.start_transaction()
    wal.log_insert('987-65-4321', {'State': 'TX', 'Occupation': 'Doctor'})
    wal.log_update('987-65-4321', {'State': 'TX', 'Occupation': 'Doctor'},
                   {'State': 'TX', 'Occupation': 'Surgeon'})
    wal.log_delete('987-65-4321', {'State': 'TX', 'Occupation': 'Surgeon'})
    wal.end_transaction()

    # Load WAL entries
    transactions = wal.load_wal()
    print(transactions)


# Run the test
test_wal()
