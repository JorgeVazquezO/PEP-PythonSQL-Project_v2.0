from pathlib import Path
import csv
import sqlite3

#print("\n********************************************************************\n")

#forces the program to establish the working directory path into the current folder
BASE_DIR=Path(__file__).resolve().parent

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users(BASE_DIR/"users.csv")
    load_and_clean_call_logs(BASE_DIR/"callLogs.csv")
    write_user_analytics(BASE_DIR/"userAnalytics.csv")
    write_ordered_calls(BASE_DIR/"orderedCalls.csv")

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path,"r") as my_file:
        csv_reader=csv.reader(my_file)
        header=next(csv_reader)
        #size=len(header)
        cursor.execute("""
            PRAGMA table_info(users)
        """)
        header_names=[col[1] for col in cursor.fetchall() if col[5]==0]
        header_count=len(header_names)
        for row in csv_reader:
            row_clean=[]
            for item in row:
                row_clean.append(item.strip())
            if(len(row_clean)==header_count and "" not in row_clean):
                cursor.execute("""
                    INSERT INTO users (firstname,lastname) VALUES (?,?)""",(row_clean)
                )

    #print("TODO: load_users")
    #cursor.execute("""SELECT * FROM users""")
    #print(cursor.fetchall())
    #cursor.execute("""SELECT * FROM users""")
    #print(f"number of entires in users list:{len(cursor.fetchall())}")


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path,"r") as my_file:
        csv_reader=csv.reader(my_file)
        header=next(csv_reader)
        #size=len(header)
        cursor.execute("""
            PRAGMA table_info(callLogs)
        """)
        header_names=[col[1] for col in cursor.fetchall() if col[5]==0]
        header_count=len(header_names)
        for row in csv_reader:
            row_clean=[]
            for item in row:
                row_clean.append(item.strip())
            if(len(row_clean)==header_count and "" not in row_clean):
                cursor.execute("""
                    INSERT INTO callLogs (phoneNumber,startTime,endTime,direction,userId) VALUES (?,?,?,?,?)""",(row_clean)
                )
                #clean_logs.append(row)
    #print("TODO: load_call_logs")
    #cursor.execute("""SELECT * FROM callLogs""")
    #print(cursor.fetchall())
    #cursor.execute("""SELECT * FROM callLogs""")
    #print(f"number of entries in callLogs list:{len(cursor.fetchall())}")

# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    cursor.execute("""
        SELECT * FROM callLogs
    """)
    callLogs_list=cursor.fetchall()
    #print(callLogs_list)
    
    log_start={}
    log_end={}
    log_num={}
    call_avrg_dur={}

    for entry in callLogs_list: 
        userid=int(entry[5])
        if userid in log_num:
            log_start[userid]+=int(entry[2])
            log_end[userid]+=int(entry[3])
            log_num[userid]+=1
        else:
            log_start[userid]=int(entry[2])
            log_end[userid]=int(entry[3])
            log_num[userid]=1
    
    for us_id in log_start:
        call_avrg_dur[us_id]=(log_end[us_id]-log_start[us_id])/log_num[us_id]

    clean_data=[]
    for line in log_num:
        clean_data.append([line,call_avrg_dur[line],log_num[line]])

    with open(csv_file_path,"w",newline="") as my_csv_file:
        fieldnames=['userId','avgDuration','numCalls']
        csvwriter = csv.writer(my_csv_file)
        csvwriter.writerow(fieldnames)
        csvwriter.writerows(clean_data)

    #print("TODO: write_user_analytics")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute("""
        SELECT * FROM callLogs ORDER BY userId, startTime
    """)
    clean_logs=cursor.fetchall()
    cursor.execute("""
            PRAGMA table_info(callLogs)
        """)
    header_names=[col[1] for col in cursor.fetchall()]
    with open(csv_file_path,"w",newline="") as my_csv_file:
        csvwriter=csv.writer(my_csv_file)
        csvwriter.writerow(header_names)
        csvwriter.writerows(clean_logs)

    #print("TODO: write_ordered_calls")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
