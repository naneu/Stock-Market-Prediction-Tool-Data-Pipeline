# Stock Market Prediction Tool  Data Pipeline

## Incremental data pipeline with apache airflow and postgres database

Wikipedia is one of the largest public information resources on the Internet. In addition to wiki pages, other items such pageview counts  are also publicly available. 

The Wikimedia foundation provides  all pageviews since 2015. the pageviews can be downloaded in a gzip format and aggregated per hour per page.

In this project therefore, we will build a data pipeline for a (fictitious) stock market prediction tool that applies sentiment analysis. For the purposes of this example, we will apply the axiom that an increase in a company's pageviews shows a positive sentiment, and the company's stock is likely to  increase. On the hand decrease in pageviews tells a loss in interest and stock price is likely to decrease.  we will use a few sample companies for the demonstration. we will also focus on the following company names {google, amazon, apple, microsoft, facebook} to help drive the idea home.

#### Data

from the screenshot below we can see the urls follow a fixed pattern. we can use this to download the data in an incremental fashion.





#### Workflow



#### How to run

1. ensure that the additional postgres package is installed and create table in postgres for storing the output

```
CREATE TABLE pageview_counts  (

	pagename VARCHAR(50) NOT NULL,

	pageviewcount INT NOT NULL,

	datetime TIMESTAMP NOT NULL

);


```

check out the documentation for reference in creating postgres connection https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html

2.  start the metastore 

   ```
   airflow db init
   ```

3. on different terminals start the webserver and scheduler

   ```
   airflow scheduler
   ```

   ```
   airflow webserver
   ```

   

#### Output















After a number of DAG runs, the Postgres database will contain a few records extracted from the Wikipedia pageviews. Once an hour, Airflow now automatically downloads the new hourly pageviews data set, unzips it, extracts the desired counts, and writes these to the Postgres database. We can now ask questions such as “At which hour is each page most popular?”

```
SELECT x.pagename, x.hr AS "hour", x.average AS "average pageviews"
FROM (
SELECT
pagename,
date_part('hour', datetime) AS hr,
AVG(pageviewcount) AS average,
ROW_NUMBER() OVER (PARTITION BY pagename ORDER BY AVG(pageviewcount) DESC)
FROM pageview_counts
GROUP BY pagename, hr
) AS x
WHERE row_number=1;
```

the results of the above query: 





With the above query, we have completed the envisioned Wikipedia workflow, which performs a full cycle of downloading the hourly pageview data, processing the data, and writing results to a Postgres database for future analysis.

