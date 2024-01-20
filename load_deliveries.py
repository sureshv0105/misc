from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

fake = Faker()

# Mapping of city codes to city name
city_mapping = {
    "BHM": "Birmingham",
    "DHN": "Dothan",
    "HSV": "Huntsville",
    "MOB": "Mobile",
    "MGM": "Montgomery",
    "ANC": "Anchorage",
    "FAI": "Fairbanks",
    "JNU": "Juneau",
    "FLG": "Flagstaff",
    "PHX": "Phoenix",
    "TUS": "Tucson",
    "YUM": "Yuma",
    "FYV": "Fayetteville",
    "LIT": "Little Rock",
    "XNA": "Northwest Arkansas",
    "BUR": "Burbank",
    "FAT": "Fresno",
    "LGB": "Long Beach",
    "LAX": "Los Angeles",
    "OAK": "Oakland",
    "ONT": "Ontario",
    "PSP": "Palm Springs",
    "SMF": "Sacramento",
    "SAN": "San Diego",
    "SFO": "San Francisco",
    "SJC": "San Jose",
    "SNA": "Santa Ana",
    "ASE": "Aspen",
    "COS": "Colorado Springs",
    "DEN": "Denver",
    "GJT": "Grand Junction",
    "PUB": "Pueblo",
    "BDL": "Hartford",
    "HVN": "Tweed New Haven",
    "IAD": "Washington, DC",
    "DCA": "Washington, DC",
    "DAB": "Daytona Beach",
    "FLL": "Fort Lauderdale",
    "RSW": "Fort Meyers",
    "JAX": "Jacksonville",
    "EYW": "Key West",
    "MIA": "Miami",
    "MCO": "Orlando",
    "PNS": "Pensacola",
    "PIE": "St. Petersburg",
    "SRQ": "Sarasota",
    "TPA": "Tampa",
    "PBI": "West Palm Beach",
    "PFN": "Panama City"
}

merchant_ids = ['M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'M12']
consumer_ids = ['CON1', 'CON2', 'CON3', 'CON4', 'CON5', 'CON6', 'CON7', 'CON8', 'CON9', 'CON10', 'CON11', 'CON12']

# Connect to your PostgreSQL database
connection = psycopg2.connect(
    host="localhost",
    user="delivery",
    password="delivery",
    database="delivery"
)

cursor = connection.cursor()

# Generate and insert 5000 records with city codes
records = []
for _ in range(5000):
    city_code = fake.random_element(elements=list(city_mapping.keys()))
    city_name = city_mapping.get(city_code)

    # Generate delivery date within the next 30 days
    delivery_date = fake.date_time_between_dates(
        datetime.now(),
        datetime.now() - timedelta(days=30)
    )

    record = (
        fake.uuid4(),  # delivery_id
        delivery_date,  # delivery_date
        fake.random_element(elements=('Placed', 'Completed', 'Cancelled')),  # delivery_status
        fake.random_element(elements=('Delivery', 'Pickup')),  # order_type
        fake.pydecimal(left_digits=5, right_digits=2, positive=True),  # total_amount
        fake.random_element(elements=merchant_ids),  # merchant_id
        fake.random_int(min=1, max=5, step=1),  # merchant_rating
        fake.random_element(elements=consumer_ids),  # consumer_id
        city_code,  # city_id
        city_name,  # city_name
    )
    records.append(record)

# Define the SQL INSERT statement
insert_query = """
    INSERT INTO fact_deliveries (
        delivery_id, delivery_date, delivery_status, order_type, total_amount,
        merchant_id, merchant_rating, consumer_id, city_id, city_name
    ) VALUES %s
"""

# Execute the INSERT statement with the generated records
execute_values(cursor, insert_query, records, template=None, page_size=100)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
