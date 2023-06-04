import requests
import json
from datetime import datetime
import pymysql
import config


def etl():
    # MySQL connection details & API stuff
    url = config.api_url
    api_key = config.api_key
    # List of train stations - add if needed
    train_stations = ['Sub', 'Bkb', 'Sci', 'Sod']

    # XML to json
    def fetch_data(station): 
        xml = f'''
            <REQUEST>
                <LOGIN authenticationkey="{api_key}" />
                <QUERY objecttype="TrainAnnouncement" schemaversion="1.3" orderby="AdvertisedTimeAtLocation">
                    <FILTER>
                        <AND>
                            <EQ name="ActivityType" value="Avgang" />
                            <EQ name="LocationSignature" value="{station}" />
                            <OR>
                                <AND>
                                    <GT name="AdvertisedTimeAtLocation" value="$dateadd(-00:15:00)" />
                                    <LT name="AdvertisedTimeAtLocation" value="$dateadd(00:30:00)" />
                                </AND>
                                <AND>
                                    <LT name="AdvertisedTimeAtLocation" value="$dateadd(00:30:00)" />
                                    <GT name="EstimatedTimeAtLocation" value="$dateadd(-00:15:00)" />
                                </AND>
                            </OR>
                        </AND>
                    </FILTER>
                    <INCLUDE>AdvertisedTrainIdent</INCLUDE>
                    <INCLUDE>AdvertisedTimeAtLocation</INCLUDE>
                    <INCLUDE>TrackAtLocation</INCLUDE>
                    <INCLUDE>ToLocation</INCLUDE>
                    <INCLUDE>Canceled</INCLUDE>
                </QUERY>
            </REQUEST>
        '''
        headers = {'Content-Type': 'application/xml'}

        response = requests.post(url, data=xml, headers=headers)
        response_dump = json.loads(json.dumps(response.json()))

        # Create a list to store the records
        records = []

        # Connect to the MySQL database
        conn = pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database
        )
    
        cursor = conn.cursor()
        try:
            # Loop through the parsed JSON data and collect the records
            for record in response_dump["RESPONSE"]["RESULT"][0]["TrainAnnouncement"]:
                trainId = record.get("AdvertisedTrainIdent")
                avgang = str(record["AdvertisedTimeAtLocation"])  # Convert to string
                destination = record.get("ToLocation", [{"LocationName": ""}])[0].get("LocationName", "")
                canceled = record.get("Canceled")
        
                # Change datetime format from yyyy-mm-ddthh:mm:ssz to yyyy-mm-dd hh:mm
                original_date_string = avgang
                original_format = "%Y-%m-%dT%H:%M:%S.%f%z"
                new_format = "%Y-%m-%d %H:%M"
                original_datetime = datetime.strptime(original_date_string, original_format)
                new_date_string_avgang = original_datetime.strftime(new_format)
                            
                # Changes station name from "signature names" to ordinary names in SQL database
                if station == "Cst": 
                    station_name = "Stockholms Centralstation"
                elif station == "Sod":
                    station_name = "Stockholm Odenplan"
                elif station == "Sci":
                    station_name = "Stockholm City"
                elif station == "Sub":
                    station_name = "Sundbyberg"
                elif station == "Bkb":
                    station_name = "Barkarby"
                    
                # Check if the record already exists in the database
                check_query = "SELECT 1 FROM TrainInfo WHERE from_station = %s AND departure_time = %s LIMIT 1"
                cursor.execute(check_query, (station_name, new_date_string_avgang))
                exists = cursor.fetchone()
                    
                if not exists:
                    # Add the record to the list
                    records.append((station_name, new_date_string_avgang, destination, trainId, canceled))
        
            if records:
                # Insert all the new records into the TrainInfo table
                insert_query = "INSERT INTO TrainInfo (from_station, departure_time, end_destination, train_number, cancelled) " + \
                               "VALUES (%s, %s, %s, %s, %s)"
                cursor.executemany(insert_query, records)
                conn.commit()
                print("Inserted records from station " + str(records[0][0]))
                conn.commit()
                print("Inserted records from station " + str(records[0][0]))
            else:
                print("No new records to insert for station " + str(station_name))
        except pymysql.Error as e:
            print("Failed to insert records into TrainInfo table:", str(e))
            conn.rollback()
            
        # Close the connection
        conn.close()
    
    # Fetch data for each train station
    for station in train_stations:
        fetch_data(station)
            
# Call the etl() function to start the ETL process
etl()

