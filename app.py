from functions import LibraryManager

if __name__ == '__main__':
    manager_on_disk = LibraryManager("db_file.db") 
    manager_on_disk.create_tables()

    manager_in_memory = LibraryManager()
    manager_in_memory.create_tables()