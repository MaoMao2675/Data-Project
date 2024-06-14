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

# generate SSN set first for unique records then conver to list
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
df.set_index('SSN', inplace=True)
print(df)

# Create directories if they don't exist
BUCKETS_DIR = './buckets_v2'
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





