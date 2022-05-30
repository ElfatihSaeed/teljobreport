import os
import sys
import telebot
import time
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
import sqlite3
import psycopg2
import schedule
import time


load_dotenv()

API_KEY = os.getenv('API_KEY')
bot = telebot.TeleBot(API_KEY)
DATABASE_URL = os.getenv('DATABASE_URL')


def get_subscribers():
  # conn = sqlite3.connect('sudanjobsearch.db')
  conn = psycopg2.connect(DATABASE_URL, sslmode='require')
  cur = conn.cursor()
  cur.execute(f'select * from tbl_subscribers')
  subs = cur.fetchall()
  cur.close()
  conn.close()
  return subs


def search_send_jobs(user_id,keywords,sites):
  for keyword in keywords:
    keyword = keyword.strip()
    if 'sudancareers' in sites:
      jobs_list = ''
      site_url  = f'https://www.sudancareers.com/job-vacancies-search-sudan/{keyword}?'
      print(f"Searching sudancareers for '{keyword}' at {site_url}\n")
      html_text = requests.get(site_url).text
      soup = BeautifulSoup(html_text,'lxml')
      jobs = soup.find_all('div',class_='job-description-wrapper',limit=1)
      for job in jobs:
        job_title = job.find('h5').text.strip()
        job_comp_date = job.find('p',class_='job-recruiter').text.split('|')
        job_post_date = job_comp_date[0]
        company_name = job_comp_date[1]
        location = job.find('div',class_='col-lg-5 col-md-5 col-sm-5 col-xs-12 job-title').text.split('\n')[4].replace('Region of : ','').strip()
        job_details = 'https://www.sudancareers.com' + job.find('h5').a['href']
        jobs_list +=  f'Job Title   : {job_title}\n' + \
                      f'Company  : {company_name}\n' +  \
                      f'Location    : {location}\n' + \
                      f'<a href="{job_details}">More Details</a> \n'
      print(jobs_list)
      print(f'------------------------------------------\n')
      if jobs_list != '':
        try:
          bot.send_message(user_id,jobs_list,parse_mode = "html",disable_web_page_preview=True)
        except :
          print("Unexpected error:", sys.exc_info()[0],'\n')
      
    if 'orooma' in sites:
      jobs_list = ''
      site_url = f'https://orooma.com/jobs/all/?q={keyword}' 
      print(f"Searching orooma for '{keyword}' at {site_url}\n")
      html_text = requests.get(site_url).text
      soup = BeautifulSoup(html_text,'lxml')
      jobs = soup.find_all('div',class_='card_group_item job_group_item',limit=1)
      for job in jobs:
        company_name = job.find('div',class_='m_0 p_0').text.strip()
        job_details = 'https://orooma.com' + job.find('a',class_='p_light overflow_auto d_block')['href']
        job_title = job.find('h4',class_='text_primary m_0').text.strip()
        jobs_list +=  f'Job Title   : {job_title}\n' + \
                      f'Company  : {company_name}\n' +  \
                      f'<a href="{job_details}">More Details</a> \n' 
      print(jobs_list)
      print(f'------------------------------------------\n')
      if jobs_list != '':
        try:
          bot.send_message(user_id,jobs_list,parse_mode = "html",disable_web_page_preview=True)
        except :
          print("Unexpected error:", sys.exc_info()[0],'\n')

    if 'sudanjob' in sites:
      jobs_list = ''
      site_url = f'https://www.sudanjob.net/' 
      print(f"Searching sudanjob for '{keyword}' at {site_url}\n")
      html_text = requests.get(site_url).text
      soup = BeautifulSoup(html_text,'lxml')
      jobs = soup.find_all('td' ,class_="module flex-module")
      for job in jobs:
        job_title = job.find('a',class_='a_homelist').text.strip()
        company_name = job.find('font',class_='sudanjob_orang_color').text.strip()
        location = job.find('div',align="left").text.split(' ')[4].strip()
        job_details = 'https://www.sudanjob.net/' + job.find('a',class_='btn_class')['href']
        if str(job_title).lower() == str(keyword).lower():
          jobs_list +=  f'Job Title   : {job_title}\n' + \
                        f'Company  : {company_name}\n' +  \
                        f'Location  : {location}\n' +  \
                        f'<a href="{job_details}">More Details</a> \n' 
      print(jobs_list)
      print(f'------------------------------------------\n')
      if jobs_list != '':
        try:
          bot.send_message(user_id,jobs_list,parse_mode = "html",disable_web_page_preview=True)
        except :
          print("Unexpected error:", sys.exc_info()[0],'\n')
    
  bot.send_message(user_id,'To unsubscribe from daily report\nsend /Unsubscribe\nto change watch list please\nsend /Change')      


def daily():
  sites = ['sudancareers','orooma','sudanjob']
  print(sites)
  subs = get_subscribers()
  for sub in subs:
    sub_id = sub[0]
    sub_jobs = sub[4].split(',')
    print('Processing sub ',sub_id,sub_jobs)
    search_send_jobs(sub_id,sub_jobs,sites)

schedule.every().day.at("10:30").do(daily)

while True:
  schedule.run_pending()
  time.sleep(600)