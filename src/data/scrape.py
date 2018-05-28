#!/usr/bin/python3

import urllib3
from bs4 import BeautifulSoup
import re
import requests
import pandas as pd
import numpy as np
import sys
import time
import argparse

PAGE_RE = re.compile(r'/artist/[0-9]+')
HOST_NAME = "http://www.artprice.com"
urllib3.disable_warnings()
HTTP = urllib3.PoolManager()

USERNAME = r'info@chenghuai.org'
PASSWORD = r'Meixuehui1!' 

search_url = 'https://www.artprice.com/artist/231215/wang-zhan/lots/pasts?ipp=25&nbd=5&p=%s&sort=datesale_desc'
login_url = 'https://www.artprice.com/login/login?layout=1'

class Scraper():
	def __init__(self):
		self.sess = requests.Session()

		auth = {"login": USERNAME, "pass": PASSWORD, "utf8": "&#x2713;"}
		r = self.sess.post( login_url, data=auth)
		

	def get_one_item(self, url):
		out = self.sess.get(url)
		return parse_one(out.content)

	def get_max_page(self, url):
		out = self.sess.get(url)
		return parse_max_pages(out.content)


	def get_items(self,url):
		# get all links to invididual items from a search result
		response = self.sess.get(url)
		soup = BeautifulSoup(response.content,"html5lib")
		boxes = soup.find_all('a')
		refs = [link.get('href') for link in boxes]
		refs = [ref for ref in refs if ref and re.match(PAGE_RE,ref)]
		refs = [ref for ref in refs if ref and not re.search(r'/lots/',ref) ]
		return set(refs)

	
def parse_one(item_source):
	# parse data from html string
	soup = BeautifulSoup(item_source,"html5lib")
	out = {}
	pairs = soup.find_all("div", {"class":"col-xs-4"}), soup.find_all("div", {"class":"col-xs-8"})
	for k,v in zip(pairs[0],pairs[1]):
		out[re.sub('\s+',' ',k.text).strip()] = re.sub('\s+',' ',v.text).strip()
	return out


def parse_max_pages(search_page):
	# get maximum #pages from search result page
	soup = BeautifulSoup(search_page,"html5lib")
	page_info = soup.find("div",{'class':'artp-pagination visible-xs'})
	page_node = page_info.find_all('li',{'class':re.compile(r'^page$')})
	n_pages = page_node[-1].text
	return int(n_pages)
	

def scrape_all_pages(save_path, save_every_50=False):
	scraper = Scraper()
	n_pages = scraper.get_max_page(search_url%'1')
	print('Start scraping %s pages...'%n_pages)
	item_list = []
	c = 0
	for idx in range(n_pages): 
		refs = scraper.get_items(search_url%idx)
		for ref in refs:
			url = HOST_NAME + ref
			item = scraper.get_one_item(url)
			time.sleep(0.5)
			if item:
				item_list.append(item)
			c+=1

			if c%50 == 0 and save_every_50:
				print("Saving %s items to %s"%(c, save_path))
				df = pd.DataFrame(item_list)
				df.to_csv(save_path+"_"+str(c)+'.csv')
	df = pd.DataFrame(item_list)
	df.to_csv(save_path+".csv")
	return df



#### Data Cleaning ###
class Cleaner():
	def __init__(self, dataframe, price_unit = '$'):
		self.df = dataframe
		self.clean_date = lambda x: pd.to_datetime(x)
		self.clean_artist = lambda x: re.sub(r'Add to my favorite artists','',x)
		self.clean_size = lambda x: ([float(a.split()[0]) for a in re.findall(r'\s[\d|.]+\scm',x)] + ['cm'] if x is not np.nan else np.nan)
		self.clean_hammer_price = lambda x: x if not re.findall(r'Lot not sold', x) else np.nan
		self.price_unit = price_unit
		self.clean_price = lambda x: self.extract_price(self.price_unit, x)
		self.clean_fns = {"Auction date": self.clean_date, 
							"Artist": self.clean_artist,
							"Size": self.clean_size, 
							"Hammer price": self.clean_hammer_price, 
							"High estimate": self.clean_price,
							"Low estimate": self.clean_price,
							"Hammer price": self.clean_price,
							"Price including buyer's premium with/without taxes": self.clean_price}
		

	def extract_price(self, unit, x):
		# unit: "$"|"CNY" etc
		if x is np.nan:
			return x
		else:
			nums =re.findall(re.escape(unit)+r'\s([\d|.|,]+)\s',x)
			if len(nums)==0:
				return np.nan
			else:
				num = re.sub("[^\d\.]", "", nums[0])
			return float(num)

	def clean(self):
		df = self.df.drop_duplicates()
		for name, fn in self.clean_fns.items():
			attrib = df[name]
			try:
				new_attrib = attrib.apply(fn)
				df[name] = new_attrib
			except Exception as e:
				print(name)
				print (e)
				continue
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
		self.df = df
		return df

def clean_data(df):
	cleaner = Cleaner(df)
	df = cleaner.clean() 
	return df



def main(args):
	if not args.resume_clean:
		df = scrape_all_pages(args.outfile+"_dirty",save_every_50=args.save_every_50)
	else:
		df = pd.read_csv(args.outfile+"_dirty.csv")
	
	df = clean_data(df)
	df.to_csv(args.outfile+"_clean.csv", index=False)


def make_args():
	parser = argparse.ArgumentParser("Scrape all items from search results, "+
										"save raw data in `outfile`_dirty.csv and clean data in `outfile`_clean.csv")
	parser.add_argument('--save-every-50', action="store_true",
							help='save to output file after scraping every 50 items')
	parser.add_argument("--resume-clean", action="store_true",
							help = 'resume cleaning for the output file')

	parser.add_argument("outfile", help='output file name')

	args = parser.parse_args()
	return args

if __name__=="__main__":
	args = make_args()
	main(args)

	
	