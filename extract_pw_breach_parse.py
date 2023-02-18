import pandas
import csv
import os
import sqlite3

def extract_pw(source=".\\data\\", shadow_path=".\\pd_data_shadow\\", remove_files=False):
    '''
    @param:
        source: folder where the source files are
        destination: destination folder
        remove_files: if set to True, the program removes the files from source
    @description:
        This methods filters out all the pw with the regex separator [because I dont use the mail, it will destroy the mail address]
        I have seen that there are lines with separator like (few of them) ";" or (most of them) "," but this can also be in the  password.
        You will end up with multiple columns. But you can append the pieces together after splitting them.
        After that it will remove the original files from the source path (because you will end up in to much data, and I don't have the free space).
        this will make ~42GB of data to ~13.6 GB amount of data
    '''
    for root, subdirs, files in os.walk(source):
        for name in files:
            df = pandas.read_csv(os.path.join(root, name), encoding="latin1", on_bad_lines="skip", delimiter=None, names=["mail", "pw"], quoting=csv.QUOTE_NONE, sep="@[\w.]+:", engine="python")
            df["pw"].to_csv(os.path.join(shadow_path, name), index=False, header=False, escapechar="\\")
            if remove_files:
                try:
                    os.remove(os.path.join(root, name))
                except:
                    pass

def preprocess_unique(source=".\\pd_data_shadow\\", destination=".\\pd_unique\\", remove_files=False):
    '''
    @param:
        source: folder where the source files are
        destination: destination folder
        remove_files: if set to True, the program removes the files from source
    @description:
        This method does read the files and count unique PW's
        It does reduce the 13.6GB to 10.4GB
        from something like this:\n
        File1:\n
        abcd\n
        abcd\n
        1234\n
        w64\n
        ----to output location File1----\n
        abcd\t2\n
        1234\t1\n
        w64\t1\n
    '''
    file_counter = 0
    for root, subdirs, files in os.walk(source):
        for name in files:
            df = pandas.read_csv(os.path.join(root, name), encoding="latin1", on_bad_lines="skip", delimiter=None, names=["pw"], quoting=csv.QUOTE_NONE, escapechar="\\")
            df = df["pw"].value_counts()
            # using a tabulator to seperate the passwords from the count else using a standard "," or ";" would cause in multiple columns because a password can contain such chars too
            df.to_csv(os.path.join(destination, name), header=False, sep="\t")
            file_counter += 1
            if remove_files:
                try:
                    os.remove(os.path.join(root, name))
                except:
                    pass

def sqldatabase_creation(source=".\\pd_unique\\", db=".\\pd_unique_db\\database.db", remove_files=False):
    '''
   
    @param:
        source: folder where the source files are
        db: database file
        remove_files: if set to True, the program removes the files from source
    @description:
        This method creates the database and "table1" from the unique files, with two columns: pw and count
    '''
    print("Creating database and feeding information")
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE table1 (pw TEXT, count INT);")
    
    for count, name in enumerate(os.listdir(source)):
        print(name, count)
        with open(os.path.join(source, name), "r", encoding="latin1") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                cursor.execute("INSERT INTO table1 VALUES (?, ?);", row)
        if remove_files:
            try:
                os.remove(os.path.join(source, name))
            except:
                pass
    # Commiting changes and close connection
    conn.commit()
    conn.close()

def unique_db(db=".pd_unique_db\\database.db"):
    '''
    @param:
        db: database file
    @description:
        This method creates table2. With the unique amount of pw and the sum of multiple entries.
        like this:
        ('123456', 100)
        ('123456789', 1020)
        ('qwerty', 163)
        After this execution the table2 is present and the .db file is about 25.4GB large 
    '''
    # Connect to db
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    # Create table2
    cursor.execute("CREATE TABLE table2 (pw TEXT, count INT);")

    # sum the unique values of table1
    cursor.execute("SELECT pw, SUM(count) AS sum FROM table1 GROUP BY pw;")
    # store them in table2
    for row in cursor.fetchall():
        cursor.execute("INSERT INTO table2 VALUES (?, ?);", row)

    # Commiting changes and close connection
    conn.commit()
    conn.close()

def db_create_stats(db=".\\pd_unique_db\\database.db", destination=".\\pd_unique_db\\"):
    '''
    @param:
        db: database file
        destination: location to store statistics
    @description:
        This method creates predefined statistics about the database/table2
    '''
    # Connect to db
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # top first 100k pw
    cursor.execute("SELECT pw FROM table2 ORDER BY count DESC LIMIT 100000")
    with open(os.path.join(destination, "top_100_k.txt"), "a", encoding="latin1") as f:
        for row in cursor.fetchall():
            f.write(row[0])
            f.write("\n")

    # top 200k pw
    cursor.execute("SELECT pw FROM table2 ORDER BY count DESC LIMIT 200000")
    with open(os.path.join(destination, "top_200_k.txt"), "a", encoding="latin1") as f:
        for row in cursor.fetchall():
            f.write(row[0])
            f.write("\n")

    # top 1 mio pw
    cursor.execute("SELECT pw FROM table2 ORDER BY count DESC LIMIT 1000000")
    with open(os.path.join(destination, "top_1_mio.txt"), "a", encoding="latin1") as f:
        for row in cursor.fetchall():
            f.write(row[0])
            f.write("\n")

    #some stats
    cursor.execute("SELECT COUNT(pw) FROM table2")
    with open(os.path.join(destination, "stats.txt"), "a", encoding="latin1") as f:
        f.write("Amount of unique passwords:")
        for row in cursor.fetchall():
            f.write(str(row[0]))
            f.write("\n")
    cursor.execute("SELECT SUM(count) FROM table2")
    with open(os.path.join(destination, "stats.txt"), "a", encoding="latin1") as f:
        f.write("Amount of passwords:")
        for row in cursor.fetchall():
            f.write(str(row[0]))
            f.write("\n")
    # Commiting changes and close connection
    conn.commit()
    conn.close()

def db_drop_table1(db=".\\database.db"):
    '''
    @param:
        db: database file
        destination: location to store statistics
    @description:
        After table2 has been created, table1 isn't needed anymore.
        This reduces the database size from 25.6GB to 9.11 GB
    '''
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE table1;")
    cursor.execute("VACUUM;")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # extracting the passwords from all the files
    extract_pw()
    # doing some preprocessing, count unique password of each file
    preprocess_unique()
    # create db and store the files in it
    sqldatabase_creation()
    # combine the unique passwords
    unique_db()
    # drop unneeded table1
    db_drop_table1()
    # creating stats, and wordlists
    db_create_stats()

    print("Done!")