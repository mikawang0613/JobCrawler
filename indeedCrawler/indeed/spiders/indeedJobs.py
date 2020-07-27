# -*- coding: utf-8 -*-
import scrapy
import csv
import mysql.connector
import glob
import os



class IndeedjobsSpider(scrapy.Spider):
    name = 'indeedJobs'
    allowed_domains = ['www.indeed.com/jobs?q=software+engineer+intern']
    start_urls = ['https://www.indeed.com/jobs?q=software+engineer+intern%2F&rbl=San+Francisco,+CA&jlid=6cf5e6d389fd6d6b']

    def parse(self, response):
        jobs = response.xpath('//*[@data-tn-component = "organicJob"]')

        for job in jobs:
            date = job.xpath('.//span[@class = "date "]/text()').extract_first()
            title = job.xpath('.//a[@data-tn-element = "jobTitle"]/@title[1]').extract_first()
            company = job.xpath('normalize-space(.//span [@class  = "company"])').extract_first()
            link = response.urljoin(job.xpath(".//h2[@class='title']//a/@href").extract_first())

            yield{'date' : date,
                  'title' : title,
                  'company' : company,
                  'link' : link}
    
    def close(self,reason):
        csv_file = max(glob.iglob('*.csv'),key = os.path.getctime)

        mydb = mysql.connector.connect(host = "localhost",user = "root",passwd = "",db = "jobs_db")
        cursor = mydb.cursor()
        csv_data = csv.reader(file(csv_file))

        insert_query = ("INSERT INTO jobs_tables "
                "(date, title, company ,link) "
                "VALUES (%s, %s, %s, %s)")

        row_count = 0 
        for row in csv_data:
            if row_count != 0:
                cursor.execute(insert_query,tuple(row))
            row_count+=1

        mydb.commit()
        cursor.close()
        mydb.close()