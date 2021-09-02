from bs4 import BeautifulSoup as soup
from threading import Thread as thd
import requests as reqs
import pandas as pd
import os

import pyodbc

if 1>2:
    server = 'tcp:myserver.database.windows.net' 
    database = 'C:/Python39/Nesco' 
    conn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database+'.accdb;')
    cursor = conn.cursor()
    insetr = "INSERT INTO NescoSites(Site, Country, Type, Link, Property, BufferZone, Description, YearAdded) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format
    insetrBasic = "INSERT INTO NescoSites(Site, Country, Type, Link) values ('{}', '{}', '{}', '{}')".format
    insetrDetailed = "INSERT INTO NescoSites(Property, BufferZone, YearAdded) values ('{}', '{}', '{}')".format

    url = 'https://whc.unesco.org/en/list/'
    country = '/statesparties/'
    site = '/list/'

def NescoSiteDataPull():
    raw = reqs.get(url)
    html = soup(raw.text, 'lxml')
    anchs = html.findAll('a', attrs={'href':True})
    cs = [x for x in anchs if country in x.get('href') or site in x.get('href')]
    cty = ''
    sites=[]


    for x in cs:
        try:
            if country in x.get('href'):
                cty = x.text
            elif cty != '':
                site = x.text.replace("'",'')
                link = "https://whc.unesco.org" + x.get('href')
                typ = x.parent.get('class')[0]
                tmpdic = {'site':site,
                          'country':cty.replace("'",''),
                          'link':link,
                          'type':typ}
                sites.append(tmpdic)
        except TypeError:
            pass

    types = {'natural_danger', 'box', 'active', 'buttons', 'cultural_danger', 'mixed', 'cultural', 'natural'}
    rejected = ['box', 'active', 'buttons']

    sites =[x for x in sites if x.get('type') not in rejected]

    for st in sites:
        cursor.execute(insetrBasic(st.get('site'),
                                    st.get('country'),
                                    st.get('type'),
                                    st.get('link')))
    return None

def GetContinents():
    raw = reqs.get('https://worldpopulationreview.com/country-rankings/list-of-countries-by-continent')
    html = soup(raw.text,'lxml')
    div = html.findAll('div',attrs={'class':'content'})
    conts = ['Africa', 'Europe', 'Asia', 'North America', 'South America', 'Australia/Oceania']
    childs = div[0].children
    chld = childs.findAll('h2') and childs.findAll('li')

    conts = ['Africa', 'Europe', 'Asia', 'North America', 'South America', 'Australia', 'Oceania']
    cc = []
    C = ''
    for c in chld:
        if c.text in conts:
            C = c.text
            print(C)
        elif C!='':
            cc.append((c.text,C))
    return None

def SQL_STUFF():
    connstr = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database+'.accdb;'
    conn = pyodbc.connect(connstr)
    cursor = conn.cursor()
    cursor.execute('ALTER TABLE NescoSites ADD continent VARCHAR(15)')
    coun = cursor.execute('SELECT country FROM NescoSites')
    cnty = coun.fetchall()
    counChangeStr = "UPDATE NescoSites SET Country = '{}' WHERE Country = '{}'".format
    contUpdateStr = "UPDATE NescoSites SET continent = '{}' WHERE country = '{}'".format
    cursor.execute("CREATE TABLE COUNTRIES (counrty VARCHAR(30), continent VARCHAR(30), gdp INT, population LONG, subdivisions INT, yearfounded INT)")
    cursor.execute("ALTER TABLE countries DROP COLUMN counrty")
    cursor.execute("ALTER TABLE countries ADD country VARCHAR(30)")
    cursor.commit()
    cursor.close()
    conn.close()
    return None

def NescoDB():
    connstr = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database+'.accdb;'
    conn = pyodbc.connect(connstr)
    cursor = conn.cursor()
    return cursor

#########################################################################################################

def updateNesco(func):
    def NescoWrap(sites):
        global failures
        failures = []
        
        UpdateN = "UPDATE NescoSites SET Property = %s, BufferZone = %s, YearAdded = %s WHERE [site-id] = %s"
        UpdateC = "INSERT INTO sitecriteria ([site-id],[criteria-id]) VALUES ('{}','{}')".format
        UpdateT = "INSERT INTO SiteText ([site-id],[textfieldnum],[words]) VALUES ('{}','{}','{}')".format
        UpdateM = "INSERT INTO SiteMisc ([site-id],[field],[miscvalue]) VALUES ('{}','{}','{}')".format

        UpdateQ, upp, upb, upy, UpdateQend = "UPDATE NescoSites SET" ,' Property = %s,' ,' BufferZone = %s,', ' YearAdded = %s', ' WHERE [site-id] = %s'
        fldpairs = list(zip(['Property','Buffer zone','Date of Inscription'],[upp, upb, upy]))

        f2c = ['Property','Buffer zone','Date of Inscription']
        crit = 'Criteria'

        connstr = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database+'.accdb;'
        connector = pyodbc.connect(connstr)
        router = conn.cursor()
        count = 0
        
        for site in sites:

            ids = site[0]
            nsvTrue,miscTrue,critTrue = [False]*3
            
            vays,txt = func(site)
            miscVays = [[m[0],m[1].translate(str.maketrans('~`!@#$%^&*()_-\/\'\"{}[]?><|', '                          '))] for m in vays if m[0] not in f2c+[crit]]
            nsVays = [ns if 'ha' not in ns[1] else (ns[0],float(''.join([n for n in ns[1].split(' ')[0] if n.isdigit() or n=='.']))) for ns in vays if ns[0] in f2c]
            critVays = [c for c in vays if c[0]=='Criteria'][0][1].replace(')(',') (').split(' ')
            
            if nsVays:
                nsVays.append(('id',ids))
                nsVays = dict(nsVays)
                vays = tuple(nsVays.get(ky) for ky in ['Property','Buffer zone','Date of Inscription','id'] if ky in nsVays.keys())
                if len(vays)==4:
                    router.execute(UpdateN%vays)
                    nsvTrue = True
                elif 'id' in nsVays.keys():
                    UpdateQN = UpdateQ + ''.join(fld[1] for fld in fldpairs if fld[0] in nsVays.keys()) + UpdateQend
                    router.execute(UpdateQN%vays)
                    nsvTrue = True

            if miscVays:
                for mv in miscVays:
                    imv = [ids]
                    imv.extend(mv)
                    router.execute(UpdateM(*imv))
                    miscTrue = True

            if critVays:
                for cv in critVays:
                    try:
                        icv = [ids,cv]
                        router.execute(UpdateC(*icv))
                        critTrue = True
                    except pyodbc.IntegrityError:
                        pass

            conn.commit()
            count += 1

            print(f"Completed Record {count} with the following entered (NescoVals, MiscVals, CritVals) = ({nsvTrue},{miscTrue},{critTrue})")
            if not any([nsvTrue,miscTrue,critTrue]):
                failures.append(site)
                print(nsVays)
                print("Failure Number: {}".format(len(failures)))
            
        router.close()
        connector.close()
        
    return NescoWrap

@updateNesco
def pullData(site):

    g = iter(range(1,200))    
    link = site[-2]
    
    if 'https:' in link:
        print(link)
        raw = reqs.get(link)
        html = soup(raw.text, 'lxml')
        text = enumerate([p.text for p in html.findAll('p')])
        div = html.findAll('div',attrs={'class':['alternate']})
        txt = div[0].text
        t = [x for x in txt.replace('\t','').replace('\r','').replace(':\n',':').split('\n') if x != '']

        t = [('misc{}'.format(next(g)),u) if ':' not in u else (u.split(':')[0].strip(' '),
                                                                u.split(':')[1].strip(' ')) for u in t]
        
        return (t,text)

####################################################################################################

database = 'C:/Python39/Nesco' 
conn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database+'.accdb;')
cursor = conn.cursor()

recs = cursor.execute("SELECT * FROM NescoSites")
recs = recs.fetchall()
rics = recs[5:]
pullData(rics)


