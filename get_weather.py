# -*- coding: utf-8 -*-
"""
This used selenium to run a web driver to download all data of Houston weather
from 1978 till 2018

Created on Thu Oct 10 10:10:48 2019

@author: deepb
"""
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
import sqlalchemy as db
import pyodbc
import urllib

if __name__ == '__main__':
    # pyodbc
    params = urllib.parse.quote_plus\
    (r'Driver={ODBC Driver 13 for SQL Server};Server=tcp:innovationxheatextremes.database.windows.net,1433;Database=Houston Weather;Uid=heat-######;Pwd=#####;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = db.create_engine(conn_str, echo=True)
    xpath = '/html/body/app/city-history/city-history-layout/div/div[2]/section/div[2]/div[3]/div/div[1]/div/div/city-history-observation/div/div[2]/table'
    
    
    for year in range(1978,2019):
        for month in range(1,13):
            URL = 'https://www.wunderground.com/history/monthly/us/tx/houston/KHOU/date/'
            date = str(year) + '-' + str(month)
            URL = URL + date
            
            driver = webdriver.Chrome()
            driver.get(URL)
            timeout = 10
            try:
                element_present = EC.presence_of_element_located((By.XPATH, xpath))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Took too much time to load: ", date)
                driver.close()
                continue
                
            
            table = driver.find_element_by_xpath(xpath).get_attribute('outerHTML')
            raw_df = pd.read_html(table)
            
            max_df = pd.DataFrame(columns = [column for column in raw_df[0].columns])
            max_df["Time"] = raw_df[1][0][1:]
            max_df["Temperature (° F)"] = raw_df[2][0]
            max_df["Dew Point (° F)"] = raw_df[3][0]
            max_df["Humidity (%)"] = raw_df[4][0]
            max_df["Wind Speed (mph)"] = raw_df[5][0]
            max_df["Pressure (Hg)"] = raw_df[6][0]
            max_df["Precipation (in)"] = raw_df[7]
            
            min_df = pd.DataFrame(columns = [column for column in raw_df[0].columns])
            min_df["Time"] = raw_df[1][0][1:]
            min_df["Temperature (° F)"] = raw_df[2][2]
            min_df["Dew Point (° F)"] = raw_df[3][2]
            min_df["Humidity (%)"] = raw_df[4][2]
            min_df["Wind Speed (mph)"] = raw_df[5][2]
            min_df["Pressure (Hg)"] = raw_df[6][2]
            min_df["Precipation (in)"] = raw_df[7]
            
            avg_df = pd.DataFrame(columns = [column for column in raw_df[0].columns])
            avg_df["Time"] = raw_df[1][0][1:]
            avg_df["Temperature (° F)"] = raw_df[2][1]
            avg_df["Dew Point (° F)"] = raw_df[3][1]
            avg_df["Humidity (%)"] = raw_df[4][1]
            avg_df["Wind Speed (mph)"] = raw_df[5][1]
            avg_df["Pressure (Hg)"] = raw_df[6][1]
            avg_df["Precipation (in)"] = raw_df[7]
            
            driver.quit()
            
            max_df.to_sql('max-'+date, engine, if_exists = 'replace')
            avg_df.to_sql('avg-'+date, engine, if_exists = 'replace')
            min_df.to_sql('min-'+date, engine, if_exists = 'replace')
    
    
    

