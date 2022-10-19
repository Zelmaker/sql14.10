import psycopg2
from psycopg2 import Error
from my_pass import password

def connection():
    try:
        connect = psycopg2.connect(dbname='hw3animals',
                                   user='postgres',
                                   password=password(),
                                   host='localhost')
        connect.autocommit = True
        return connect

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def execute_request(cursor, request):
    try:
        cursor.execute(request)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def create_tables(cursor):
    request = """
    CREATE TABLE  IF NOT EXISTS outcome_subtype(
        id SERIAL PRIMARY KEY,
        outcome_subtype TEXT
    );

    CREATE TABLE IF NOT EXISTS outcome_type(
        id SERIAL PRIMARY KEY,
        outcome_type TEXT
    );

    CREATE TABLE IF NOT EXISTS breed(
        id SERIAL PRIMARY KEY,
        breed TEXT
    );

    CREATE TABLE IF NOT EXISTS color(
        id SERIAL PRIMARY KEY,
        color TEXT
    );

    CREATE TABLE IF NOT EXISTS animal_type(
        id SERIAL PRIMARY KEY,
        animal_type TEXT
    );

    CREATE TABLE IF NOT EXISTS animals(
        animal_id TEXT PRIMARY KEY NOT NULL,
        fk_animal_type INT REFERENCES animal_type(id),
        name TEXT,
        fk_breed INT REFERENCES breed(id),
        fk_color1 INT REFERENCES color(id) NOT NULL,
        fk_color2 INT REFERENCES color(id),
        date_of_birth TEXT
    );

    CREATE TABLE IF NOT EXISTS shelter_info(
        id INT PRIMARY KEY,
        age_upon_outcome TEXT,
        animal_id TEXT REFERENCES animals(animal_id),
        fk_id_outcome_subtype INT REFERENCES outcome_subtype(id),
        fk_id_outcome_type INT REFERENCES outcome_type(id),
        outcome_month INT,
        outcome_year INT
    );
    """
    execute_request(cursor, request)


def insert_outcome_subtype(cursor):

    request = f"""
        INSERT INTO outcome_subtype (outcome_subtype)
                SELECT DISTINCT outcome_subtype FROM main
                WHERE outcome_subtype <> ''
    """
    execute_request(cursor, request)


def insert_outcome_type(cursor):
    request = f"""
        INSERT INTO outcome_type (outcome_type)
            SELECT DISTINCT outcome_type FROM main
            WHERE outcome_type <> ''
        """
    execute_request(cursor, request)


def insert_breed(cursor):
    request = f"""
        INSERT INTO breed (breed)
            SELECT DISTINCT breed FROM main
            WHERE breed <> ''
        """
    execute_request(cursor, request)


def insert_color(cursor):
    request = f"""
        INSERT INTO color (color)
            SELECT DISTINCT color1 FROM main
            WHERE color1 <> ''
        """
    execute_request(cursor, request)


def insert_animal_type(cursor):
    request = f"""
            INSERT INTO animal_type (animal_type)
                SELECT DISTINCT animal_type FROM main
                WHERE animal_type <> ''
            """
    execute_request(cursor, request)


def insert_animals(cursor):
    request = f"""
        INSERT INTO animals
            SELECT animal_id, animal_type.id, name, breed.id, c1.id, c2.id, date_of_birth 
                FROM main
                LEFT JOIN animal_type ON main.animal_type = animal_type.animal_type
                LEFT JOIN breed ON main.breed = breed.breed
                LEFT JOIN color as c1 ON main.color1 = c1.color
                LEFT JOIN color as c2 ON main.color2 = c2.color
                GROUP BY animal_id, animal_type.id, name, breed.id, c1.id, c2.id, date_of_birth 
            """
    execute_request(cursor, request)


def insert_shelters(cursor):
    request = """
        INSERT INTO shelter_info
            SELECT index, age_upon_outcome, animal_id, outcome_subtype.id, outcome_type.id, outcome_month, outcome_year 
            FROM main
            LEFT JOIN outcome_subtype ON main.outcome_subtype = outcome_subtype.outcome_subtype
            LEFT JOIN outcome_type ON main.outcome_type = outcome_type.outcome_type
    """
    execute_request(cursor, request)

def create_users(cursor):
    request = """
    CREATE ROLE user_2 WITH PASSWORD 'user_2_password';
    GRANT CONNECT ON DATABASE hw3animals TO user_2;
    GRANT USAGE ON ALL TABLES IN SCHEMA hw3animals TO user_2;
    GRANT SELECT ON ALL TABLES IN SCHEMA hw3animals TO user_2;

    CREATE ROLE user_admin_2 WITH PASSWORD 'admin2_password';
    GRANT CONNECT ON DATABASE hw3animals TO user_2; 
    GRANT INSERT ON ALL TABLES IN SCHEMA hw3animals TO user_admin_2;
    GRANT UPDATE ON ALL TABLES IN SCHEMA hw3animals TO user_admin_2;
    """
    execute_request(cursor, request)


def main():
    connect = connection()
    cursor = connect.cursor()

    create_tables(cursor)

    insert_outcome_subtype(cursor)
    insert_outcome_type(cursor)
    insert_breed(cursor)
    insert_color(cursor)
    insert_animal_type(cursor)
    insert_animals(cursor)
    insert_shelters(cursor)

    create_users(cursor)

    if connect:
        cursor.close()
        connect.close()


if __name__ == '__main__':
    main()

    # index, age_upon_outcome, animal_id, animal_type, name, breed, color1, color2, date_of_birth, outcome_subtype, outcome_type, outcome_month, outcome_year)