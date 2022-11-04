import pandas as pd
import mysql.connector as mysql
import csv

# Playlist completa com as aulas
# https://www.youtube.com/playlist?list=PLDqAb8tE7SQYro9KNWEitZGzyX0uHIHr5
# Canal Youtube @ProfessorMarcoMaddo

def lerCSV():
    arquivoCSV = pd.read_csv('dados.csv', index_col=False, delimiter=",")
    arquivoCSV.head()
    print(arquivoCSV)

def testarConnection():
    conn = mysql.connect(host='localhost', database='pythonCSV',
                         user='python',    password='MsBr!2022')

    print(conn.get_server_info())
    print(conn.get_server_version())


def getConnection():
    conn = mysql.connect(host='localhost', database='pythonCSV',
                         user='python', password='MsBr!2022')
    return conn

def insertCsvToMySQL():

    conn = getConnection()
    print(conn.get_server_info())

    with open("dados.csv") as arquivoCSV:
        dadosCSV = csv.reader(arquivoCSV, delimiter=",")
        dadosToMySQL = []
        for row in dadosCSV:
            dadosToMySQL.append((row[0], row[1], row[2], row[3]))
            sql_stm = "INSERT INTO contatos (id,nome,cidade,uf) value (%s, %s, %s, %s)"
            cur = conn.cursor()
            cur.executemany(sql_stm,dadosToMySQL)
            print(dadosToMySQL)
            dadosToMySQL = []
            conn.commit()
        cur.close()

if __name__ == '__main__':
    insertCsvToMySQL()

