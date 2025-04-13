import psycopg2
from datetime import datetime
from config import DATABASE_CONFIG

# Connect to the databases
def connect_db():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        exit()


def create_olap_schema(cur):
    # Drop tables if they exist
    cur.execute("""
        DROP TABLE IF EXISTS Fact_Verzuim, Dim_Personeel, Dim_Locatie, Dim_Date CASCADE;
    """)

    # Create Dim_Personeel
    cur.execute("""
        CREATE TABLE Dim_Personeel (
            id SERIAL PRIMARY KEY,
            geslacht VARCHAR,
            leeftijd INT,
            leeftijdsgroep VARCHAR,
            contract VARCHAR,
            in_opleiding BOOLEAN,
            deskundigheid VARCHAR,
            datum_in_dienst DATE,
            datum_uit_dienst DATE,
            dienststatus VARCHAR,
            dienst_duur_in_dagen INT,
            verzuim_aantal INT,
            gem_verzuim_per_maand FLOAT
        );
    """)

    # Create Dim_Locatie
    cur.execute("""
        CREATE TABLE Dim_Locatie (
            id SERIAL PRIMARY KEY,
            werk_locatie VARCHAR
        );
    """)

    # Create Dim_Date
    cur.execute("""
        CREATE TABLE Dim_Date (
            datum_key INT PRIMARY KEY,
            datum DATE,
            jaar INT,
            maand INT,
            kwartaal INT,
            weeknummer INT,
            dagnaam VARCHAR,
            is_weekend BOOLEAN
        );
    """)

    # Create Fact_Verzuim
    cur.execute("""
        CREATE TABLE Fact_Verzuim (
            id SERIAL PRIMARY KEY,
            personeel_id INT REFERENCES Dim_Personeel(id),
            locatie_id INT REFERENCES Dim_Locatie(id),
            datum_key INT REFERENCES Dim_Date(datum_key),
            verzuim VARCHAR -- e.g., 'Absent', 'Present'
        );
    """)


def populate_dim_personeel(cur):
    cur.execute("""
        INSERT INTO Dim_Personeel (id, geslacht, leeftijd, leeftijdsgroep, contract, in_opleiding,
                                   deskundigheid, datum_in_dienst, datum_uit_dienst, dienststatus, 
                                   dienst_duur_in_dagen, verzuim_aantal, gem_verzuim_per_maand)
        SELECT 
            p.id,
            p.geslacht,
            p.leeftijd,
            CASE 
                WHEN p.leeftijd BETWEEN 18 AND 25 THEN '18-25'
                WHEN p.leeftijd BETWEEN 26 AND 35 THEN '26-35'
                WHEN p.leeftijd BETWEEN 36 AND 45 THEN '36-45'
                WHEN p.leeftijd BETWEEN 46 AND 55 THEN '46-55'
                ELSE '56+'
            END AS leeftijdsgroep,
            p.contract,
            p.in_opleiding,
            p.deskundigheid,
            p.datum_in_dienst,
            p.datum_uit_dienst,
            CASE 
                WHEN p.datum_uit_dienst IS NULL OR p.datum_uit_dienst > CURRENT_DATE THEN 'Active'
                ELSE 'Inactive'
            END AS dienststatus,
            COALESCE((p.datum_uit_dienst - p.datum_in_dienst), 0) AS dienst_duur_in_dagen,
            COUNT(v.id) AS verzuim_aantal,
            CASE 
                WHEN COUNT(v.id) > 0 THEN COUNT(v.id) / GREATEST(DATE_PART('month', AGE(CURRENT_DATE, p.datum_in_dienst)), 1)
                ELSE 0
            END AS gem_verzuim_per_maand
        FROM personeel p
        LEFT JOIN verzuim v ON p.id = v.personeel_id AND v.verzuim = TRUE
        GROUP BY p.id, p.geslacht, p.leeftijd, p.contract, p.in_opleiding, p.deskundigheid,
                 p.datum_in_dienst, p.datum_uit_dienst;
    """)




def populate_dim_locatie(cur):
    cur.execute("""
        INSERT INTO Dim_Locatie (id, werk_locatie)
        SELECT id, werk_locatie
        FROM locatie;
    """)


def populate_dim_date(cur):
    cur.execute("""
        INSERT INTO Dim_Date (datum_key, datum, jaar, maand, kwartaal, weeknummer, dagnaam, is_weekend)
        SELECT
            EXTRACT(YEAR FROM d)::INT * 10000 + EXTRACT(MONTH FROM d)::INT * 100 + EXTRACT(DAY FROM d)::INT AS datum_key,
            d AS datum,
            EXTRACT(YEAR FROM d)::INT AS jaar,
            EXTRACT(MONTH FROM d)::INT AS maand,
            EXTRACT(QUARTER FROM d)::INT AS kwartaal,
            EXTRACT(WEEK FROM d)::INT AS weeknummer,
            TO_CHAR(d, 'Day') AS dagnaam,
            CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM generate_series('2023-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) d;
    """)


def populate_fact_verzuim(cur, conn, chunk_size=100000):
    offset = 0
    while True:
        cur.execute(f"""
            INSERT INTO Fact_Verzuim (id, personeel_id, locatie_id, datum_key, verzuim)
            SELECT 
                v.id,
                v.personeel_id,
                p.locatie_id,
                TO_CHAR(v.datum, 'YYYYMMDD')::INT AS datum_key,
                v.verzuim
            FROM verzuim v
            JOIN personeel p ON v.personeel_id = p.id
            ORDER BY v.id
            LIMIT {chunk_size} OFFSET {offset};
        """)

        conn.commit()

        # Break if no more rows are left to process
        rows_affected = cur.rowcount
        if rows_affected == 0:
            break

        offset += chunk_size
        #print(f"Processed {offset} rows...")


def OLTP_to_OLAP():
    # oltp_conn = connect_db()
    conn = connect_db()

    with conn.cursor() as cur:
        create_olap_schema(cur)
        populate_dim_personeel(cur)
        populate_dim_locatie(cur)
        populate_dim_date(cur)
        populate_fact_verzuim(cur, conn)

    # oltp_conn.close()
    conn.close()


if __name__ == "__main__":
    OLTP_to_OLAP()