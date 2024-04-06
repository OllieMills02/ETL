import csv
import mysql.connector
import requests

db1 = mysql.connector.connect(
    host="localhost",
    user='root',
    password='Olliem-2002',
    port='3306',
    database='etl'
)

# please put in api key here
apiKey = ""

cursor1 = db1.cursor()
queries = [
    "CREATE TABLE IF NOT EXISTS Location (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), country VARCHAR(255))",
    "CREATE TABLE IF NOT EXISTS Hotel (id INT AUTO_INCREMENT PRIMARY KEY, location_id INT, name VARCHAR(255), INDEX(name), FOREIGN KEY (location_id) REFERENCES Location(id))",
    "CREATE TABLE IF NOT EXISTS Account (account_id INT AUTO_INCREMENT PRIMARY KEY, points INT)",
    "CREATE TABLE IF NOT EXISTS Person (id INT AUTO_INCREMENT PRIMARY KEY, account_id INT, name VARCHAR(255), room_type VARCHAR(255), staying_at VARCHAR(255), FOREIGN KEY (account_id) REFERENCES Account(account_id), FOREIGN KEY (staying_at) REFERENCES Hotel(name))",
    "CREATE TABLE IF NOT EXISTS Room (id INT AUTO_INCREMENT PRIMARY KEY, type_name VARCHAR(255), occupancy INT)"
]

for query in queries:
    cursor1.execute(query)

db2 = mysql.connector.connect(
    host="localhost",
    user='root',
    password='Olliem-2002',
    port='3306',
    database='fake_people'
)

cursor2 = db2.cursor()


def addHotels(location):
    url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={apiKey}&searchQuery={location}&category=hotels&language=en"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    data = response.json().get("data")
    for hotel in data:
        name = hotel.get('name')
        address_obj = hotel.get('address_obj')
        if address_obj:
            address = address_obj.get('street1')
            country = address_obj.get('country')

        cursor1.execute("INSERT IGNORE INTO Location (address, country) VALUES (%s, %s)",
                        (address, country))
        location_id = cursor1.lastrowid

        cursor1.execute("INSERT IGNORE INTO Hotel (name, location_id) VALUES (%s, %s)", (name, location_id))
    db1.commit()


# takes form a local file to upload and transform to fit in my database
def addRoomTypes():
    with open('room_types.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')

        for row in reader:
            name = row['Name']
            occupancy = row['People']
            cursor1.execute("INSERT IGNORE INTO Room (type_name, occupancy) VALUES (%s, %s)", (name, occupancy))
            db1.commit()


cursor1.close()
cursor2.close()

cursor1 = db1.cursor()
cursor2 = db2.cursor()
def make_reservation():
    name = input("Enter your name: ")
    hotel_name = input("Enter the name of the hotel you're staying at: ")
    room_type = input("Are you staying in a Single, Double, Twin, or Queen: ")
    location = input("Enter the location of the hotel you're staying: ")
    addHotels(location)
    addRoomTypes()

    cursor2.execute("SELECT * FROM account Where name LIKE %s", (name,))
    personData = cursor2.fetchone()
    if personData is None:
        print("No account found for %s" % name)
        cursor2.execute("INSERT INTO Account (name, points) VALUES (%s, %s)", (name, 0))
        db2.commit()
        accID1 = cursor2.lastrowid
        accountPts = 1;
        cursor1.execute("INSERT IGNORE INTO Account (account_id, points) VALUES (%s, %s)", (accID1, accountPts))
        accountID = cursor1.lastrowid
    else:
        accountID = personData[0]
        accountPts = personData[2]
        cursor2.execute("UPDATE Account SET points = points + 1 WHERE idaccount = %s", (accountID,))
        db2.commit()
        cursor1.execute("INSERT IGNORE INTO Account (account_id, points) VALUES (%s, %s)", (accountID, accountPts))
    cursor1.execute("SELECT * FROM Hotel WHERE name LIKE %s", ('%' + hotel_name + '%',))
    result = cursor1.fetchone()
    print(result)
    if result is None:
        print("Incorrect Hotel name")
        print("Please pick from provided:")
        cursor1.execute("SELECT * FROM Hotel")
        all = cursor1.fetchall()
        print(all)
        exit()
    for _ in cursor1:
        pass
    print("Creating Reservation")
    # Insert reservation into Person table
    cursor1.execute("INSERT IGNORE INTO Person (name, staying_at, room_type, account_id) VALUES (%s, %s, %s, %s)",
                    (name, result[2], room_type, accountID))
    db1.commit()


make_reservation()
