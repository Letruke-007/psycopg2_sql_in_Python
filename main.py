# Домашнее задание к лекции «Работа с PostgreSQL из Python»

import psycopg2

# Создаем подключение к базе данных clients_management
conn = psycopg2.connect(database='clients_management', user='postgres', password='let007')


# Реализуем функцию создания в базе данных таблиц SQL
def createdb(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone_numbers, clients; 
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(80) NOT NULL,
            last_name VARCHAR(80) NOT NULL,
            email VARCHAR(80) UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_numbers(
            phone_number INT8 UNIQUE,
            client_id INTEGER NOT NULL REFERENCES clients(client_id)
        );     
        """)
        conn.commit()


# Реализуем функцию добавления нового клиента в базу данных
def add_client(conn, first_name, last_name, email):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(first_name, last_name, email) VALUES(%s, %s, %s);
        """, (first_name, last_name, email))
        conn.commit()


# Реализуем функцию добавления нового номера телефона в базу данных
def add_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_numbers(client_id, phone_number) VALUES(%s, %s);
        """, (client_id, phone_number))
        conn.commit()


# Реализуем функцию изменения данных о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, new_phone_number=None,
                  previous_phone_number=None):
    with conn.cursor() as cur:
        if first_name is not None:
            cur.execute("""
            UPDATE clients SET first_name=%s WHERE client_id=%s;
            """, (first_name, client_id))
            conn.commit()
        if last_name is not None:
            cur.execute("""
                        UPDATE clients SET last_name=%s WHERE client_id=%s;
                        """, (last_name, client_id))
            conn.commit()
        if email is not None:
            cur.execute("""
                        UPDATE clients SET email=%s WHERE client_id=%s;
                        """, (email, client_id))
            conn.commit()
        if new_phone_number is not None and previous_phone_number is not None:
            cur.execute("""
                        UPDATE phone_numbers SET phone_number=%s WHERE client_id=%s AND phone_number=%s;
                        """, (new_phone_number, client_id, previous_phone_number))
            conn.commit()


# Реализуем функцию удаления телефона клиента
def delete_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
           DELETE FROM phone_numbers WHERE client_id=%s AND phone_number=%s;
           """, (client_id, phone_number))
        conn.commit()


# Реализуем функцию удаления клиента
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_numbers WHERE client_id=%s;
        """, client_id)
        cur.execute("""
        DELETE FROM clients WHERE client_id=%s;
        """, client_id)
        conn.commit()


# Реализуем функцию поиска клиента в БД по его данным
def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if first_name is not None:
            cur.execute("""
            SELECT client_id, first_name, last_name, email FROM clients WHERE first_name=%s;
            """, (first_name,))
            print(cur.fetchone())
        elif last_name is not None:
            cur.execute("""
            SELECT client_id, first_name, last_name, email FROM clients WHERE last_name=%s;
            """, (last_name,))
            print(cur.fetchone())
        elif email is not None:
            cur.execute("""
            SELECT client_id, first_name, last_name, email FROM clients WHERE email=%s;
            """, (email,))
            print(cur.fetchone())
        elif phone_number is not None:
            cur.execute("""
            SELECT client_id, phone_number FROM phone_numbers WHERE phone_number=%s;
            """, (phone_number,))
            print(cur.fetchone())


# ТЕСТЫ КОДА

# Создаем таблицы в БД
createdb(conn)

# Добавляем информацию о новом клиенте
add_client(conn, 'John', 'Walker', 'john@gmail.com')
with conn.cursor() as cur:
    cur.execute("""
        SELECT * FROM clients;
        """)
    print(cur.fetchall())

# Вносим данные о номерах телефона клиента (тест дважды, чтобы проверить корректность удаления и замены впоследствии)
add_phone(conn, 1, 89647801703)
add_phone(conn, 1, 89163323752)
with conn.cursor() as cur:
    cur.execute("""
        SELECT client_id, phone_number FROM phone_numbers;
        """)
    print(cur.fetchall())

# Ищем клиента по данным о нем
find_client(conn, first_name='John')
find_client(conn, last_name='Walker')
find_client(conn, email='john@gmail.com')

# Меняем данные о клиенте (выборочно)
change_client(conn, 1, 'Albert', last_name='Einstein', email='albert@genious.com', new_phone_number=1098798755, previous_phone_number=89163323752)
with conn.cursor() as cur:
    cur.execute("""
        SELECT * FROM clients;
        """)
    print(cur.fetchall())
    cur.execute("""
        SELECT client_id, phone_number FROM phone_numbers;
        """)
    print(cur.fetchall())

# Удаляем телефон клиента (один из двух)
delete_phone(conn, 1, 89647801703)
with conn.cursor() as cur:
    cur.execute("""
        SELECT client_id, phone_number FROM phone_numbers;
        """)
    print(cur.fetchall())

# Удаляем информацию о клиенте полностью из БД
delete_client(conn, '1')
with conn.cursor() as cur:
    cur.execute("""
        SELECT * FROM clients;
        """)
    print(cur.fetchall())



