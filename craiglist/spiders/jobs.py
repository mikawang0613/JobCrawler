import scrapy
import csv
import mysql.connector
import glob
import os


class JobsSpider(scrapy.Spider):
    name = 'jobs'
    allowed_domains = ['sfbay.craigslist.org']
    start_urls = ['https://sfbay.craigslist.org/search/jjj?query=software+engineer&sort=rel']

    def parse(self, response):
        listings = response.xpath('//li[@class="result-row"]')

        for listing in listings:
            date = listing.xpath('.//*[@class = "result-date"]/@datetime').extract_first()
            link = listing.xpath('.//a[@class = "result-title hdrlnk"]/@href').extract_first()
            title = listing.xpath('.//a[@class = "result-title hdrlnk"]/text()').extract_first()

            yield{'date' : date,
                  'link' : link,
                  'title' : title}

    def close(self,reason):
        csv_file = max(glob.iglob('*.csv'),key = os.path.getctime)

        mydb = mysql.connector.connect(host = "localhost",user = "root",passwd = "",db = "jobs_db")
        cursor = mydb.cursor()
        csv_data = csv.reader(file(csv_file))

        insert_query = ("INSERT INTO jobs_tables "
                 "(date, links, title) "
                 "VALUES (%s, %s, %s)")
        
        # select_query = "SELECT * FROM jobs_table"
        # cursor.execute(select_query)
        # for (date, link, text) in cursor:
        #     print("================", date)


        row_count = 0 
        for row in csv_data:
            if row_count != 0:
                cursor.execute(insert_query,tuple(row))
            row_count+=1

        mydb.commit()
        cursor.close()
        mydb.close()
