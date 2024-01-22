import sqlite3
from sqlite3 import Error

class LibraryManager:
   def __init__(self, db_file=None):
      if db_file:
            self.db_file = db_file
            self.conn = self.create_connection()
      else:
            self.conn = self.create_connection_in_memory()

   def create_connection(self):
      self.conn = None
      try:
            conn = sqlite3.connect(self.db_file)
            return conn
      except Error as e:
            print(e)
      return conn

   def create_connection_in_memory(self):
      """ create a database connection to a SQLite in-memory database """
      conn = None
      try:
            conn = sqlite3.connect(":memory:")
            print(f"Connected, sqlite version: {sqlite3.version}")
            return conn
      except Error as e:
            print(e)
   
   def execute_sql(self, sql):
        try:
            c = self.conn.cursor()
            c.execute(sql)
        except Error as e:
            print(e)
      
   def create_tables(self):
      create_movies_sql = """
      -- movies table
      CREATE TABLE IF NOT EXISTS movies (
      id integer PRIMARY KEY,
      name TEXT NOT NULL,
      genre TEXT NOT NULL,
      description TEXT NOT NULL,
      Status VARCHAR(15) NOT NULL,
      author_id INTEGER,
      FOREIGN KEY (author_id) REFERENCES authors (author_id)
      );
      """
      self.execute_sql(create_movies_sql)

      create_author_sql = """
      -- author table
      CREATE TABLE IF NOT EXISTS authors (
      author_id INTEGER PRIMARY KEY,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      age INTEGER NULL,
      );
      """
      self.execute_sql(create_author_sql)

      create_genre_sql = """
      -- genre table
      CREATE TABLE IF NOT EXISTS genres (
      genre_id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      movie_id INTEGER,
      FOREIGN KEY (movie_id) REFERENCES movies (id)
      );
      """
      self.execute_sql(create_genre_sql)

   def add_movie(self, movie):
      sql = '''INSERT INTO movies(name, genre, description, Status, author_id )
               VALUES(?,?,?,?,?)'''
      cur = self.conn.cursor()
      cur.execute(sql, movie)
      return cur.lastrowid
   
   def add_author(self, author):
      sql = '''INSERT INTO authors(project_id, first_name, last_name, age)
             VALUES(?,?,?,?)'''
      cur = self.conn.cursor()
      cur.execute(sql, author)
      self.conn.commit()
      return cur.lastrowid

   def add_genres(self, genre):
      sql = '''INSERT INTO genres(genre_id, name, movie_id)
               VALUES(?,?,?)'''
      cur = self.conn.cursor()
      cur.execute(sql, genre)
      self.conn.commit()
      return cur.lastrowid
   
   def select_all(self, table):
   
      cur = self.conn.cursor()
      cur.execute(f"SELECT * FROM {table}")
      rows = cur.fetchall()

      return rows
   
   def select_where(self, table, **query):
      cur = self.conn.cursor()
      qs = []
      values = ()
      for k, v in query.items():
         qs.append(f"{k}=?")
         values += (v,)
      q = " AND ".join(qs)
      cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
      rows = cur.fetchall()
      return rows
   
   def update(self, table, id, **kwargs):
      parameters = [f"{k} = ?" for k in kwargs]
      parameters = ", ".join(parameters)
      values = tuple(v for v in kwargs.values())
      values += (id, )

      sql = f''' UPDATE {table}
                  SET {parameters}
                  WHERE id = ?'''
      try:
         cur = self.conn.cursor()
         cur.execute(sql, values)
         self.conn.commit()
         print("OK")
      except sqlite3.OperationalError as e:
         print(e)

   def delete_where(self, table, **kwargs):
        qs = []
        values = tuple()
        for k, v in kwargs.items():
            qs.append(f"{k}=?")
            values += (v,)
        q = " AND ".join(qs)

        sql = f'DELETE FROM {table} WHERE {q}'
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()
        print("Deleted")

   def delete_all(self, table):
        sql = f'DELETE FROM {table}'
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        print("Deleted")

