# trafikverket_etl

Extracts data using Trafikverkets API with information of specific train stations in Stockholm, including
when train arrives, departure destination, train number and whether or not the train has been cancelled.

Data is stored as json, and transformed using Python - in this specific case, only transforming station name and date time format.

Lastly, if the data has already been collected, it will not append to "records" list, to save data.


Using Apache Airflow and dags, it is scheduled to run the Python script every 5 minutes.
<img width="1439" alt="image" src="https://github.com/alihamra/trafikverket_etl/assets/135516165/c0f758df-d1c3-47e0-95a5-b215a5c25175">

Loaded into a RDBMS in this case, mySQL will give this output in a tabular format:
<img width="604" alt="image" src="https://github.com/alihamra/trafikverket_etl/assets/135516165/b0e9617b-865c-4566-b5f9-1e2ffa9b7937">


TrainInfo Table:
>id (Primary Key): Unique identifier for each record
>
>from_station: Name of the departure station
>
>departure_time: Departure time in the format "YYYY-MM-DD HH:MM"
>
>end_destination: Name of the destination station
>
>train_number: Train identification number
>
>cancelled: Indicates if the train is canceled (1) or not (0)
>

