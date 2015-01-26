# -*- coding: utf-8 -*-

#this program is implemented so that any datafile with variables in the first line is read
#a table which suits the data are then created
#program is based om test_add_data

from __future__ import unicode_literals
import sqlite3 as lite
import sys, csv, codecs	

########################
# CONNECTS TO DATABASE #
########################
def tilfoy_tabell():
    database = 'test.db'    #databasen er presatt til test.db
    print 'Connects to database %s' % database
    try:
        con = lite.connect(database)
        cur = con.cursor()

        cur.execute ('''INSERT INTO MANGA(VAR1, VAR2) VALUES('FIST OF THE NORTH STAR', '1598')''')
        con.commit()
    except lite.Error, e:
        sys.exit(1)

#tilfoy_tabell()


###########################
#  CREATES A NEW DATABASE #
###########################
def lag_tabell():
    try:
  
        con = None
        con = lite.connect('test.db')
        cur = con.cursor()

        cur.execute('''CREATE TABLE utenriks (VAR1 TEXT, VAR2 TEXT, VAR3 TEXT, VAR4 INTEGER)''')
        #cur.commit()
        #cur.execute('''INSERT INTO utenriks (VAR1, VAR2, VAR3, VAR4) VALUES('UH', 'MAT', 'SEI', 999) ''')

#        data = cur.fetchall()
#       for element in data:
#            print element
#            for streng in element:
#                print 'data er som folger %s' % (str(streng))

        lite.commit()
        con.close()

    except lite.Error, e:
        sys.exit(1)

#lag_tabell()

####################
# CREATE NEW TABLE #
####################
def lag_ny_tabell(ny_tabell):

    con = lite.connect('test.db')
    
    with con: 
        cur = con.cursor()
        print "Oppretter tabellen %s" % ny_tabell
        cur.execute("CREATE TABLE IF NOT EXISTS %s (VAR1 TEXT, VAR2 TEXT, VAR3 TEXT, VAR4 INTEGER)" % (ny_tabell) )

lag_ny_tabell('utenriks2')

##############
# DROP TABLE #
##############
def drop_tabell(tabell):
    con = lite.connect('test.db')
    with con:
        cur = con.cursor()
        print "Sletter tabellen %s " % (tabell)
        cur.execute("DROP TABLE %s" % (tabell) )

#drop_tabell('utenriks2')


###########################
# UPLOADS DATA INTO TABLE #
###########################
def upload_data(innfil):
    '''Denne funksjonen laster opp data i en definert tabell, virker nå'''
    con = lite.connect('test.db')

    'opens the file'
    #data = codecs.open(innfil, 'r', encoding=('UTF-8'))
    data = open(innfil, 'r')
    liste2 = []
    lis_lis = []
    #fjerner dobbeltappostrof og appenderer til listen
    for linje in data:
        #linje = linje.translate(None, '"') #removes double apostrophes from text
        liste = linje.rstrip().split(';')
        #makes sure numbers stored as string are converted in the list
        for teller in range (0, len(liste)):
            if teller == 3: 
                try:
                    liste2.append(int(liste[teller]))
                except:
                    liste2.append(0)
            else:
                liste2.append(liste[teller])
        lis_lis.append(liste2)
        liste2 = [] #nullstiller listen

    with con:   
        print "Setter inn data i tabellen"    

        try:
            cur = con.cursor()
            'didnt work earlier, likely cause was illegal characters in indata'
            print u'Dette må leses!!!'
            #for element in lis_lis:  #this statement works, but not executemany
            #    print 'Setter inn i tabellen'
            #    cur.execute("INSERT INTO utenriks2(var1, var2, var3, var4) VALUES(?, ?, ?, ?)", (element))
            cur.executemany("INSERT INTO utenriks(var1, var2, var3, var4) VALUES(?, ?, ?, ?)", (lis_lis))

            con.commit()
        except:
            print 'Error'
            sys.exit(1)

#upload_data('utenriks.dat')


#####################
# PYTONSK INNLESING #
#####################
def pytonsk_csv_innlesing():
    con = lite.connect('test.db')

    cur = con.cursor()

    with open('utenriks.dat') as fin:
        dr = csv.reader(fin, delimiter=';', quotechar='|')
        til_db = [(i['var1'], i['var2'], i['var3'], i['var4']) for i in dr]

    cur.executemany("INSERT INTO utenriks2 (var1, var2, var3, var4) VALUES(?, ?, ?, ?);", til_db )

#pytonsk_csv_innlesing()


################################
# READ PARAMETERS FOR DATAFILE #
################################
def create_dict(linje):
    '''creates a dictionary with the variabel names and the variable type - string, integer etc'''
    ordbok = {}
    for teller in range (len(linje)): 
        ordbok[linje[teller]] = ''
    return ordbok

def variable_type(liste):
    type_liste = []
    missingtegn = '-' '.'
    for element in liste:
        if element.isdigit():
            type_liste.append('Int')
        elif element in (missingtegn):
            type_liste.append('Missing')
        elif isinstance(element, basestring):
            type_liste.append('String')
        else:
            type_liste.append('Unknown')
    print type_liste
    return type_liste
    

def read_parameters(innfil):
    inndata = open(innfil, 'r')
    #variable declarations
    linjenummer = 0
    ordbok = {}
    type_liste = []
    #processes  data
    for linje in inndata:
        var_liste = linje.rstrip().split(';')
        #the first line contains the variable names
        if linjenummer == 0: #only the first line
            ordbok = create_dict(var_liste)
        else:
            type_liste = variable_type(var_liste)
        linjenummer += 1

    lengde = len(var_liste)
    
    print lengde
    print ordbok
read_parameters('utenriks.dat')





###########################
# UPLOADS DATA INTO TABLE #
# Modifisert versjon      #
###########################
def upload_data(innfil):   
    '''Denne funksjonen er den samme som ovenfor men er nu modifisert'''
    '''Den er nå laget generisk slik at databasen som opprettes er av størrelse med det man skal ha'''
    '''leser inn variabelnavn fra starten og oppretter database ut fra dette. Teller opp antall variabler'''
    con = lite.connect('test.db')

    'opens the file'
    #data = codecs.open(innfil, 'r', encoding=('UTF-8'))
    data = open(innfil, 'r')
    liste2 = []
    lis_lis = []
    #fjerner dobbeltappostrof og appenderer til listen
    linjenummer = 1
    for linje in data:
        if linjenummer == 1:
            
#        linje = linje.translate(None, '"') #removes double apostrophes from text
            liste = linje.rstrip().split(';')
        #makes sure numbers stored as string are converted in the list
        for teller in range (0, len(liste)):
            if teller == 3: 
                try:
                    liste2.append(int(liste[teller]))
                except:
                    liste2.append(0) #for å lese inn blanke verdier riktig
            else:
                liste2.append(liste[teller])
        lis_lis.append(liste2)
        liste2 = [] #nullstiller listen

    with con:   
        print "Setter inn data i tabellen"    

        try:
            cur = con.cursor()
            'didnt work earlier, likely cause was illegal characters in indata'
            print 'Dette må leses!!!'
            #for element in lis_lis:  #this statement works, but not executemany
            #    print 'Setter inn i tabellen'
            #    cur.execute("INSERT INTO utenriks2(var1, var2, var3, var4) VALUES(?, ?, ?, ?)", (element))
            cur.executemany("INSERT INTO utenriks3(var1, var2, var3, var4) VALUES(?, ?, ?, ?)", (lis_lis))

            con.commit()
        except:
            print 'Error'
            sys.exit(1)

#lag_ny_tabell('utenriks3')
#upload_data('utenriks.dat')


