import pyodbc
import sys, csv, os, json
import pandas as pd
import itertools as IT
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress
from collections import defaultdict


def exec_query(query_text,item,driver= "{ODBC Driver 13 for SQL Server};",server= "REMOVED;",
               database= "REMOVED;",UID= "REMOVED;",PWD= "REMOVED;",return_value= True):
    conn= pyodbc.connect(
        r'DRIVER='+driver+
        r'SERVER='+server+
        r'DATABASE='+database+
        r'UID='+UID+
        r'PWD='+PWD
        )
    cursor= conn.cursor()
    cursor.execute(query_text)
    if return_value:
        with open('RPMresults'+item+'.csv', 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(column[0] for column in cursor.description)
            for row in cursor:
                w.writerow(row)
        data= cursor.fetchall()
        conn.close()
        return [dict(zip(columns,row)) for row in data]
    else:
        conn.commit()
        conn.close()
        return
def exec_query2(query_text,item,driver= "{ODBC Driver 13 for SQL Server};",server= "REMOVED;",
               database= "REMOVED;",UID= "REMOVED;",PWD= "REMOVED;",return_value= True):
    conn= pyodbc.connect(
        r'DRIVER='+driver+
        r'SERVER='+server+
        r'DATABASE='+database+
        r'UID='+UID+
        r'PWD='+PWD
        )
    cursor= conn.cursor()
    cursor.execute(query_text)
    if return_value:
        with open('SlopeFinder'+item+'.csv', 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(column[0] for column in cursor.description)
            for row in cursor:
                w.writerow(row)
        data= cursor.fetchall()
        conn.close()
        return [dict(zip(columns,row)) for row in data]
    else:
        conn.commit()
        conn.close()
        return
def main():
    country_codes = ['US','GB','CA','IN','Other']
    for item in country_codes:
        query =  ("""SELECT
                        advertiser
                        , age_bucket
                        , platform
                        , ISNULL(1000 * SUM(revenue) / NULLIF(CAST(SUM(receives) AS FLOAT),0),0) AS RPM
                    FROM (
                        SELECT
                            CASE
                                WHEN country = 'US' THEN 'US'
                                WHEN country = 'GB' THEN 'GB'
                                WHEN country = 'IN' THEN 'IN'
                                WHEN country = 'CA' THEN 'CA'
                                ELSE 'Other'
                            END AS country
                            , CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END AS advertiser
                            , Reports.dbo.fn_get_age_bucket('advertiser', NULL, revenuestream,widget_id) AS age_bucket
                            , platform
                            , SUM(revenue) AS revenue
                            , 0 AS receives
                            , 0 AS clicks
                        FROM
                            Reports.dbo.DailyRevenue
                        WHERE 1=1
                            AND [date] BETWEEN CAST(DATEADD(DAY,-14,GETDATE()) AS DATE) AND CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
                            AND category = 'push'
                            AND CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END IN ('AdBlade', 'MGID' , 'ContentAd' , 'RevContent', '5th ave news' )
                        GROUP BY
                            [date]
                            , country
                            , CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END
                            , Reports.dbo.fn_get_age_bucket('advertiser',NULL,revenuestream,widget_id)
                            , platform
                        UNION
                        SELECT
                            CASE
                                WHEN country = 'US' THEN 'US'
                                WHEN country = 'GB' THEN 'GB'
                                WHEN country = 'IN' THEN 'IN'
                                WHEN country = 'CA' THEN 'CA'
                                ELSE 'Other'
                            END AS country
                            , advertiser
                            , Reports.dbo.fn_get_age_bucket('userclass',DATEDIFF(DAY,userclass,[date]),NULL,NULL) AS age_bucket
                            , platform
                            , 0 AS revenue
                            , SUM(receives) AS receives
                            , SUM(clicks) AS clicks
                        FROM
                            Reports.dbo.UserMetrics_Rollup WITH (NOLOCK)
                        WHERE 1=1
                            AND CAST(DATEADD(HOUR,-4,Timeslice) AS DATE) BETWEEN CAST(DATEADD(DAY,-14,GETDATE()) AS DATE) AND CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
                        GROUP BY
                            country
                            , advertiser
                            , Reports.dbo.fn_get_age_bucket('userclass',DATEDIFF(DAY,userclass,[date]),NULL,NULL)
                            , platform
                        HAVING
                            SUM(receives) > 500
                    ) rpm_table
                    WHERE 1=1
                    AND country = '""" + item + """'
                    AND advertiser != 'Unknown'
                    GROUP BY
                        advertiser
                        , platform
                        , age_bucket
                    HAVING
                        ISNULL(1000 * SUM(revenue) / NULLIF(CAST(SUM(receives) AS FLOAT),0),0) > 0
                        AND ISNULL(1000 * SUM(revenue) / NULLIF(CAST(SUM(receives) AS FLOAT),0),0) < 15
                    ORDER BY
                        age_bucket
                        , [platform]""")
        query2 = ("""SELECT
                        CASE
							WHEN date = CAST(DATEADD(DAY,-14,GETDATE()) AS DATE) THEN 1
                            WHEN date = CAST(DATEADD(DAY,-13,GETDATE()) AS DATE) THEN 2
                            WHEN date = CAST(DATEADD(DAY,-12,GETDATE()) AS DATE) THEN 3
                            WHEN date = CAST(DATEADD(DAY,-11,GETDATE()) AS DATE) THEN 4
                            WHEN date = CAST(DATEADD(DAY,-10,GETDATE()) AS DATE) THEN 5
                            WHEN date = CAST(DATEADD(DAY,-9,GETDATE()) AS DATE) THEN 6
                            WHEN date = CAST(DATEADD(DAY,-8,GETDATE()) AS DATE) THEN 7
							WHEN date = CAST(DATEADD(DAY,-7,GETDATE()) AS DATE) THEN 8
                            WHEN date = CAST(DATEADD(DAY,-6,GETDATE()) AS DATE) THEN 9
                            WHEN date = CAST(DATEADD(DAY,-5,GETDATE()) AS DATE) THEN 10
                            WHEN date = CAST(DATEADD(DAY,-4,GETDATE()) AS DATE) THEN 11
                            WHEN date = CAST(DATEADD(DAY,-3,GETDATE()) AS DATE) THEN 12
                            WHEN date = CAST(DATEADD(DAY,-2,GETDATE()) AS DATE) THEN 13
                            WHEN date = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) THEN 14
                            ELSE 1
                        END AS [date]
                        , advertiser
                        , age_bucket
                        , platform
                        , ISNULL(1000 * SUM(revenue) / NULLIF(CAST(SUM(receives) AS FLOAT),0),0) AS RPM
                    FROM (
                        SELECT
                            [date]
                            , country
                            , CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END AS advertiser
                            , Reports.dbo.fn_get_age_bucket('advertiser', NULL, revenuestream,widget_id) AS age_bucket
                            , platform
                            , SUM(revenue) AS revenue
                            , 0 AS receives
                            , 0 AS clicks
                        FROM
                            Reports.dbo.DailyRevenue
                        WHERE 1=1
                            AND [date] BETWEEN CAST(DATEADD(DAY,-14,GETDATE()) AS DATE) AND CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
                            AND category = 'push'
                            AND CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END IN ( 'MGID' , 'ContentAd' , 'RevContent', '5th ave news', 'AdBlade' )
                        GROUP BY
                            [date]
                            , country
                            , CASE WHEN OfferType = 'indirect' THEN '5th Ave News' ELSE revenuestream END
                            , Reports.dbo.fn_get_age_bucket('advertiser', NULL, revenuestream,widget_id)
                            , platform
                        UNION
                        SELECT
                            CAST(DATEADD(HOUR,-4,Timeslice) AS DATE) [date]
                            , country
                            , advertiser
                            , Reports.dbo.fn_get_age_bucket('userclass',DATEDIFF(DAY,userclass,[date]),NULL,NULL) AS age_bucket
                            , platform
                            , 0 AS revenue
                            , SUM(receives) AS receives
                            , SUM(clicks) AS clicks
                        FROM
                            Reports.dbo.UserMetrics_Rollup WITH (NOLOCK)
                        WHERE 1=1
                            AND CAST(DATEADD(HOUR,-4,Timeslice) AS DATE) BETWEEN CAST(DATEADD(DAY,-14,GETDATE()) AS DATE) AND CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
                        GROUP BY
                            CAST(DATEADD(HOUR,-4,Timeslice) AS DATE)
                            , country
                            , advertiser
                            , Reports.dbo.fn_get_age_bucket('userclass',DATEDIFF(DAY,userclass,[date]),NULL,NULL)
                            , platform
                        HAVING
                            SUM(receives) > 500
                    ) rpm_table
                    WHERE 1=1
                    AND country = '""" + item + """'
                	AND advertiser != 'Unknown'
    				AND advertiser = 'MGID' OR advertiser = 'ContentAd' OR advertiser = 'RevContent' OR advertiser = 'AdBlade'
                    GROUP BY
                        date
                		, advertiser
                        , platform
                        , age_bucket
                    HAVING
                        ISNULL(1000 * SUM(revenue) / NULLIF(CAST(SUM(receives) AS FLOAT),0),0) < 15
                    ORDER BY
                        date,
                        platform,
                        age_bucket""")
        ###########################################################################
        # Add RPM % Column
        ###########################################################################
        dictionary_RPM = exec_query(query, item)
        dfResults = pd.read_csv('RPMresults'+ item +'.csv')
        groups = dfResults.groupby(['age_bucket','platform']).apply(lambda x:x.RPM/x.RPM.sum())
        group_pcts = pd.DataFrame({'percentiles':list(groups)})
        dfResults = dfResults.merge(group_pcts, left_index = True, right_index = True)
        dfResults.to_csv('RPMresults'+ item +'.csv')
        ###########################################################################
        # Combine with the Slope column
        ###########################################################################
        slope_RPM = exec_query2(query2, item)
        dfSlopes = pd.read_csv('SlopeFinder'+ item +'.csv')
        slopes = dfSlopes.groupby(['advertiser','age_bucket','platform']).apply(lambda v: linregress(v.date, v.RPM)[0])
        slopes_column = pd.DataFrame({'Slope':list(slopes)})
        dfSlopes = dfResults.merge(slopes_column, left_index = True, right_index = True)
        dfSlopes.to_csv('RPMresults'+ item +'.csv')
        ###########################################################################
        # Combine two datatables so that we have both slope and RPM % of the total
        ###########################################################################
        dfCalculator = pd.read_csv('RPMresults'+ item +'.csv')
        slopes_pcts = dfCalculator.groupby(['age_bucket','platform']).apply(lambda x:x.Slope/x.Slope.sum())
        list_multiplied = [a*(100+b)/100 for a,b in zip(dfCalculator['percentiles'].tolist(),slopes)]
        list_multiplied2 = [a*a*(100+b)/100 for a,b in zip(dfCalculator['percentiles'].tolist(),slopes)]
        pcts_column = pd.DataFrame({'Pre100Slope': list(list_multiplied)})
        pcts_column2 = pd.DataFrame({'Pre100RPM': list(list_multiplied2)})
        dfCalculator = dfSlopes.merge(pcts_column, left_index = True, right_index = True)
        dfCalculator2 = dfCalculator.merge(pcts_column2, left_index = True, right_index = True)
        dfCalculator2.to_csv('RPMresults'+ item +'.csv')
        ###########################################################################
        # Weighted by Slope
        ###########################################################################
        dfBalancer = pd.read_csv('RPMresults'+ item +'.csv')
        balanced_pcts = dfCalculator2.groupby(['age_bucket','platform']).apply(lambda x:x.Pre100Slope/x.Pre100Slope.sum())
        balanced_col = pd.DataFrame({'Recommended Based on Slope': list(balanced_pcts)})
        dfBalancer = dfCalculator2.merge(balanced_col, left_index = True, right_index = True)
        dfBalancer.to_csv('RPMresults'+ item +'.csv')
        ###########################################################################
        # Weighted by RPM
        ###########################################################################
        dfBalancer2 = pd.read_csv('RPMresults'+ item +'.csv')
        balanced_pcts2 = dfCalculator2.groupby(['age_bucket','platform']).apply(lambda x:x.Pre100RPM/x.Pre100RPM.sum())
        balanced_col2 = pd.DataFrame({'Recommended Based on RPM': list(balanced_pcts2)})
        dfBalancer2 = dfBalancer.merge(balanced_col2, left_index = True, right_index = True)
        dfBalancer2.to_csv('RPMresults'+ item +'.csv')
        ###########################################################################
        # Convert to Json
        ###########################################################################
        reader = csv.reader(open('RPMresults'+ item +'.csv', 'r'))
        next(reader)
        data_dict = []
        dictionary = {}
        ending = {}
        for possibility in dfBalancer2.advertiser.unique():
            dictionary[possibility] = {}
            for possibility2 in dfBalancer2.age_bucket.unique():
                dictionary[possibility][possibility2] = {}
                for possibility3 in dfBalancer2.platform.unique():
                    dictionary[possibility][possibility2][possibility3] = {}
        for row in reader:
            data_dict = [{'RPM':row[4]}, {'percentiles':row[5]},{'slope':row[6]},{'pre100s_dict':row[7]},{'pre100r_dict':row[8]},{'recs_dict':row[9]},{'recr_dict':row[10]}]
            dictionary[row[1]][row[2]][row[3]] = data_dict
        ending[item] = dictionary
        with open('results'+item+'.json','w') as fp:
             json.dump(ending,fp, sort_keys=True)
    with open('finalResults.json','w') as f:
        tempfiles = ['resultsUS.json','resultsGB.json','resultsCA.json','resultsIN.json','resultsOther.json']
        f.write('[')
        count = 0
        for tempfile in tempfiles:
            q = open(tempfile,'r')
            f.write(q.read())
            if count != 4:
                f.write(',')
            count+=1
        f.write(']')
if __name__ == '__main__':
    main()
    input("Process Finished, please close the window")
    os._exit(1)
