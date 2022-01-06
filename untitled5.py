# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 18:24:20 2020

@author: mir_h
"""
import pandas as pd
import pyodbc 
import xml.etree.ElementTree as ET

import csv

server='HASNAIN2020'
database='BIBS_XML'

def connect_to_sql_server():
    #connect to sql server
    print("Connecting to the server")
    odbc_conn=pyodbc.connect('DRIVER={SQL SERVER};SERVER='+server+';Trusted_Connection=yes;')
    odbc_conn.autocommit=True
    cursor=odbc_conn.cursor()
    
    #create db if does not exist
    transaction="IF DB_ID('{0}') IS  NULL CREATE DATABASE {0};".format(database)
    cursor.execute(transaction)
    if(cursor==True):
        print("created  db")
    
    transaction="USE {0}".format(database)
    cursor.execute(transaction)
    if(cursor==True):
        print("USe  db")
    
    #drop table if exists
    transaction="IF OBJECT_ID('dbo.ADDRESS') IS NOT  NULL DROP TABLE dbo.ADDRESS;"
    cursor.execute(transaction)
    transaction="IF OBJECT_ID('dbo.AUTHOR') IS NOT  NULL DROP TABLE dbo.AUTHOR;"
    cursor.execute(transaction)
    transaction="IF OBJECT_ID('dbo.BIBLIOGRAPHY') IS NOT  NULL DROP TABLE dbo.BIBLIOGRAPHY;"
    cursor.execute(transaction)
    transaction="IF OBJECT_ID('dbo.BIBS') IS NOT  NULL DROP TABLE  dbo.BIBS;"
    cursor.execute(transaction)
    
    
    
    
    #create Bibs table 
    transaction="IF OBJECT_ID('dbo.BIBS') IS NULL CREATE TABLE dbo.BIBS (BIB_ID VARCHAR(10) PRIMARY KEY,BIB_NAME VARCHAR(15) );"
    cursor.execute(transaction)
    print("bibs table created")
    

    #create Bibliography table
    transaction="IF OBJECT_ID('dbo.BIBLIOGRAPHY') IS  NULL CREATE TABLE dbo.BIBLIOGRAPHY (ITEM_ID VARCHAR(10) PRIMARY KEY, ITEM_TYPE VARCHAR(10), PRICE VARCHAR(7), PUBLISHER VARCHAR(45), TITLE VARCHAR(75), YEAR INT, BIB_ID VARCHAR(10) NOT NULL,CONSTRAINT BIBLIOGRAPHY_fk_group FOREIGN KEY (BIB_ID) REFERENCES dbo.BIBS(BIB_ID));"
    cursor.execute(transaction)
    print("Bibliography table created")
    
    #create author table
    transaction="IF OBJECT_ID('dbo.AUTHOR') IS  NULL CREATE TABLE dbo.AUTHOR (AUTH_ID VARCHAR(20) PRIMARY KEY, AUTH_NAME VARCHAR(60), ITEM_ID VARCHAR(10) NOT NULL, CONSTRAINT AUTHOR_fk_group FOREIGN KEY (ITEM_ID) REFERENCES dbo.BIBLIOGRAPHY(ITEM_ID));"
    cursor.execute(transaction)
    print("author table created")
    
    #create address table
    transaction="IF OBJECT_ID('dbo.ADDRESS') IS  NULL CREATE TABLE dbo.ADDRESS (ZIP INT, STREET VARCHAR(60), AUTH_ID VARCHAR(20) NOT NULL, CONSTRAINT ADDRESS_fk_group FOREIGN KEY (AUTH_ID) REFERENCES dbo.AUTHOR(AUTH_ID));"
    cursor.execute(transaction)
    print("address table created")
    
    return cursor




tree = ET.parse('BibInputFile.xml')
root = tree.getroot()


bibs_df=pd.DataFrame(columns=["bib_id", "bib_name"])
bibliography_df=pd.DataFrame(columns=["item_id","item_type","price","publisher","title","year", "bib_id"])
author_df=pd.DataFrame(columns=["auth_id","auth_name", "item_id"])
address_df=pd.DataFrame(columns=["zip", "street","auth_id"])

cursor=connect_to_sql_server()

for iteration,bib in enumerate(list(root)):
    
    bib_id=iteration+1
    print(bib.tag,bib_id)
    items=list(bib)
    #bibs_df=bibs_df.append([bib_id,bib.tag.strip()],ignore_index=True)
    
    #inserting into bibs table
    transaction="INSERT INTO dbo.BIBS VALUES( '{0}','{1}');".format(bib_id,bib.tag)
    cursor.execute(transaction) 
    
    #writing to csv file
    with open('bibs.csv','a') as bibsfile:
        writer=csv.DictWriter(bibsfile,["bib_id", "bib_name"])
        if iteration==0:
            writer.writeheader()
        writer.writerow({'bib_id':bib_id,'bib_name':bib.tag})
        
    for iteration2,item in enumerate(items):
        
        item_id=bib_id+(iteration2+1)/10
        print(item.tag,item_id)
        #print(list(item))
        item_type=item.tag
        price=item.attrib.get('price')
        publisher=item.find('publisher').text
        title=item.find('title').text
        year=item.find('year').text
        #bibliography_df=bibliography_df.append([item_id,item_type,price,publisher,title,year,bib_id],ignore_index=True)
        print(item_id,item_type,price,publisher,title,year,bib_id)
        #inserting into bibiliography table
        transaction="INSERT INTO dbo.BIBLIOGRAPHY VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}');".format(item_id,item_type,price,publisher,title,year,bib_id)
        cursor.execute(transaction) 
    
        #writing to csv file
        with open('bibliography.csv','a') as itemsfile:
            writer=csv.writer(itemsfile)
            writer.writerow([item_id,item_type,price,publisher,title,year,bib_id])
        
        #getting the authors
        authors=item.findall('author')
        for iter3, author in enumerate(authors):
            auth_id=item_id+(iter3+1)*0.01
            auth_id=round(auth_id,2)
            
            authNames=list(author)
            if(authNames==[]):
                auth_name=(author.text).strip()
            else:
                auth_name=[a.text for a in authNames if a.text]
                auth_name=(' ').join(auth_name)
            print(auth_id,auth_name,item_id)
            #inserting into authors table
            transaction="INSERT INTO dbo.AUTHOR VALUES('{0}','{1}','{2}');".format(auth_id,auth_name,item_id)
            cursor.execute(transaction) 
        
            #writing to csv file
            with open('authors.csv','a') as authorsfile:
                writer=csv.writer(authorsfile)
                writer.writerow([auth_id,auth_name,item_id])
                
             #getting the address of authors
            addresses=author.find('address')
            if addresses:
                zipcode=addresses.find('zip').text
                street=addresses.find('street').text
                print(zipcode,street,auth_id)                
                #inserting into ADDRESS table
                transaction="INSERT INTO dbo.ADDRESS VALUES('{0}','{1}','{2}');".format(zipcode,street,auth_id)
                cursor.execute(transaction) 
            
                #writing to csv file
                with open('address.csv','a') as addressfile:
                    writer=csv.writer(addressfile)
                    writer.writerow([zipcode,street,auth_id])
            
                
           
            
          
    
    



