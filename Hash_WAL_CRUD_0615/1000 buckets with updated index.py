import json
from faker import Faker
import pandas as pd
import random
import pyarrow
import hashlib
import os
from Hash_1000_Buckets import SSN

# initialize Faker
fake = Faker()

num_records = 1000000

# generate SSN set first for unique records then convert to list
ssns = set()
while len(ssns) < num_records:
    ssns.add(fake.ssn())
ssns = list(ssns)

# generate state list
state = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID',
    'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN',
    'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND',
    'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT',
    'VA', 'WA', 'WV', 'WI', 'WY'
]
random_state = random.choices(state, k=num_records)

# occupation list
occupation = [
    'Accountant', 'Actor', 'Actuary', 'Administrative Assistant',
    'Aerospace Engineer', 'Agricultural Engineer', 'Air Traffic Controller',
    'Aircraft Mechanic', 'Airline Pilot', 'Anesthesiologist',
    'Anthropologist', 'Architect', 'Archivist', 'Art Director',
    'Astronomer', 'Athletic Trainer', 'Audiologist', 'Author',
    'Automotive Mechanic', 'Baker', 'Bartender', 'Biochemist',
    'Biomedical Engineer', 'Brickmason', 'Broadcast Technician'
]
random_occupation = random.choices(occupation, k=num_records)

# generate dataframe
df = pd.DataFrame({'SSN': ssns,
                   'State': random_state,
                   'Occupation': random_occupation
                   })

# create primary index with SSN
df.set_index('SSN', inplace=True, drop=False)
print(df)

# Create directories if they don't exist
BUCKETS_DIR = './buckets_v4'
os.makedirs(BUCKETS_DIR, exist_ok=True)

# Store the DataFrame in a Parquet file
df.to_parquet(os.path.join(BUCKETS_DIR, 'primary_data.parquet'), index=True)
print('DataFrame has been saved to parquet file.')

# create secondary index with State and Occupation and store in JSON

state_index = {}
occupation_index = {}

for ssn, row in df.iterrows():
    state = row['State']
    occupation = row['Occupation']
    state_index.setdefault(state, set()).add(ssn)
    occupation_index.setdefault(occupation, set()).add(ssn)

# Convert sets to lists for JSON serialization
state_index = {k: list(v) for k, v in state_index.items()}
occupation_index = {k: list(v) for k, v in occupation_index.items()}

# Store secondary indexes in JSON files
with open(os.path.join(BUCKETS_DIR, 'state_index.json'), 'w') as f:
    json.dump(state_index, f)

with open(os.path.join(BUCKETS_DIR, 'occupation_index.json'), 'w') as f:
    json.dump(occupation_index, f)

print("Primary data and indexes are created.")


# hashing function to distribute to 1000 buckets
def hash_index_to_bucket(ssn, num_buckets=1000):
    hash_object = hashlib.md5(str(ssn).encode())
    bucket = int(hash_object.hexdigest(), 16) % num_buckets
    return bucket


# Initialize buckets and secondary indexes
buckets = {i: [] for i in range(1000)}
state_index = {i: {} for i in range(1000)}
occupation_index = {i: {} for i in range(1000)}

# Distribute rows into buckets
for ssn, row in df.iterrows():
    bucket = hash_index_to_bucket(ssn)
    buckets[bucket].append(row)

    state = row['State']
    occupation = row['Occupation']
    state_index[bucket].setdefault(state, set()).add(ssn)
    occupation_index[bucket].setdefault(occupation, set()).add(ssn)

# Save each bucket into a Parquet file and their respective secondary indexes into JSON files
for bucket, data in buckets.items():
    if data:
        bucket_df = pd.DataFrame(data)
        bucket_df.reset_index(drop=True, inplace=True)
        bucket_df.set_index('SSN', inplace=True, drop=False)
        bucket_df.sort_index()
        bucket_df.to_parquet(os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet'), index=True)

        # Convert sets to lists for JSON serialization
        state_index[bucket] = {k: list(v) for k, v in state_index[bucket].items()}
        occupation_index[bucket] = {k: list(v) for k, v in occupation_index[bucket].items()}

        with open(os.path.join(BUCKETS_DIR, f'state_index_{bucket}.json'), 'w') as f:
            json.dump(state_index[bucket], f)

        with open(os.path.join(BUCKETS_DIR, f'occupation_index_{bucket}.json'), 'w') as f:
            json.dump(occupation_index[bucket], f)

print("file and indexes are distributed to 1000 buckets")

# validation


# Directory containing the buckets
BUCKETS_DIR = './buckets_v4'


# Function to validate a single bucket
def validate_bucket(bucket_id):
    bucket_file = os.path.join(BUCKETS_DIR, f'bucket_{bucket_id}.parquet')
    state_index_file = os.path.join(BUCKETS_DIR, f'state_index_{bucket_id}.json')
    occupation_index_file = os.path.join(BUCKETS_DIR, f'occupation_index_{bucket_id}.json')

    print(f"Validating bucket {bucket_id}...")

    # Check if Parquet file exists
    if os.path.exists(bucket_file):
        print(f"Parquet file for bucket {bucket_id} exists.")
        # Load and print the first few rows of the Parquet file
        bucket_df = pd.read_parquet(bucket_file)
        print(bucket_df.head())
    else:
        print(f"Parquet file for bucket {bucket_id} does not exist.")

    # Check if state index JSON file exists
    if os.path.exists(state_index_file):
        print(f"State index file for bucket {bucket_id} exists.")
        # Load and print some of the state index JSON content
        with open(state_index_file, 'r') as f:
            state_index = json.load(f)
            print(f"State index for bucket {bucket_id}:")
            for state, ssns in list(state_index.items())[:5]:  # Print only first 5 entries
                print(f"  {state}: {ssns}")
    else:
        print(f"State index file for bucket {bucket_id} does not exist.")

    # Check if occupation index JSON file exists
    if os.path.exists(occupation_index_file):
        print(f"Occupation index file for bucket {bucket_id} exists.")
        # Load and print some of the occupation index JSON content
        with open(occupation_index_file, 'r') as f:
            occupation_index = json.load(f)
            print(f"Occupation index for bucket {bucket_id}:")
            for occupation, ssns in list(occupation_index.items())[:5]:  # Print only first 5 entries
                print(f"  {occupation}: {ssns}")
    else:
        print(f"Occupation index file for bucket {bucket_id} does not exist.")


# Validate all buckets
for bucket_id in range(1000):
    validate_bucket(bucket_id)
