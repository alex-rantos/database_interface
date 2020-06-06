
import mariadb
import os,sys

RATES_PATH = "/rates/"

INSERT_DIC = {
    "insertRates" : "INSERT INTO rates(query, arguments, specifier, workerName, instanceId, numInstances, currentWindowStart, \
                                       trueProcessingRate, trueOutputRate, observedProcessingRate, observedOutputRate) \
                     VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
}

class MySQL_DB(object):

    def __init__(self):
        config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',
        }

        try:
            self.conn = mariadb_connection = mariadb.connect(**config, database='stats')
        except mariadb.Error as err:
            print(err, file=sys.stderr)
            sys.exit(1)
        self.cur = self.conn.cursor()

    def __create_tables__(self):
        
        self.cur.execute("CREATE TABLE rates (id int auto_increment primary key,\
                query varchar(255) NOT NULL,\
                arguments varchar(255) NOT NULL,\
                specifier varchar(255) NOT NULL,\
                workerName  varchar(255) NOT NULL,\
                instanceId  varchar(255) NOT NULL,\
                numInstances  varchar(255) NOT NULL,\
                currentWindowStart  varchar(255) NOT NULL,\
                trueProcessingRate varchar(255) NOT NULL,\
                trueOutputRate varchar(255) NOT NULL,\
                observedProcessingRate varchar(255) NOT NULL,\
                observedOutputRate varchar(255) NOT NULL)")

        # commit the changes
        self.conn.commit()

    def __reset__(self):
        self.cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", ("mysqldata",))
        rows = self.cur.fetchall()
        for row in rows:
            print("drop table " + row[0].decode())
            self.cur.execute("DROP TABLE " + row[0].decode() + " cascade")   
        self.cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.conn.commit()
    
    def __show_tables__(self):
        self.cur.execute("SHOW TABLES")
        for (tbl,) in self.cur.fetchall(): # pre-fetch all data to free up the cursor
            print("\n===", tbl, "===\n")
            self.cur.execute(f"SELECT * FROM `{tbl}`")
            print([x[0] for x in self.cur.description]) # print field names (as a list)
            for row in self.cur: # using an iterator minimizes the memory used
                print(row) # print every row in this table (each as a tuple)

    def insert_into(self, params, query="insertRates"):
        try:
            response = self.cur.execute(INSERT_DIC[query], params)
            self.conn.commit()
            return response
        except mariadb.Error as err:
            print(err)
            return -1

    def get_record(self, attribute, sum = -1):
        """
            Returns all records that matches the given attribute.
            If sum = -1 return all records. Change value to the sum of record you want to be returned
        """
        try:
            self.cur.execute("SELECT users.cmkid FROM users WHERE userid = %s",[attribute])
            record = self.cur.fetchall() 
            if sum == -1:
                return record
            else:
                if (len(record) <= sum):
                    return record
                else:
                    return record[:sum]
        except mariadb.Error as err:
            print(err)
            return -1
        return None

    def get_attribute(self, userId):
        """
            Returns a specific value from a record.
        """
        try:
            self.cur.execute("SELECT users.cmkid FROM users WHERE userid = %s",[userId])
            record = self.cur.fetchall() 
            if (len(record) == 1):
                return record[0][0] # record = [('Nro',)]
            return None
        except mariadb.Error as err:
            print(err)
            return -1
        return None

    def record_exists(self,attribute):
        """
            Returns true if there is a record with the specified attribute otherwise returns false
        """
        try:
            self.cur.execute("SELECT * FROM ongoingUploads WHERE fileid = %s",[attribute])
            if (self.cur.fetchall()):
                return True
            else:
                return False
        except mariadb.Error as err:
            print(err)
            return -1

    def delete_record(self,attribute):
        """
        Deletes a record from table with specified attribute
        """
        try:
            self.cur.execute("DELETE FROM ongoingUploads WHERE fileid = %s",(attribute,))
            self.conn.commit()
            return 0
        except mariadb.Error as err:
            print(err)
            return -1

if __name__ == "__main__":
    db = MySQL_DB()

    if (len(sys.argv) > 1):
        if (sys.argv[1] == "restart"):
            db.__reset__()
            db.__create_tables__()
            print("Tables recreated")
            exit()
        elif (sys.argv[1] == "reset"):
            db.__reset__()
            print("Tables deleted")
            exit()
        elif (sys.argv[1] == "init"):
            db.__create_tables__()
            print("Tables created")
            exit()
        elif (sys.argv[1] == "show"):
            db.__show_tables__()
            print("Tables were shown")
            exit()
        elif (sys.argv[1] == "insert"):
            print("Insertion completed")
            exit()
        elif (sys.argv[1] == "exists"):
            print(db.record_exists(41))
            exit()
        elif (sys.argv[1] == "get"):
            print(db.get_record(10))
            exit()
        elif (sys.argv[1] == "del"):
            print(db.delete_record(10))
            exit()
        else:
            print("Wrong argument")