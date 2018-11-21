from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

import twitter_credentials as tc, json, time
import re, tldextract, pymongo
from prettytable import PrettyTable
from pymongo import MongoClient

try:
	import urllib2 as liburl
except NameError:
	import urllib.request as liburl


class TwitterStreamer():

	def __init__(self):
		self.auth = OAuthHandler(tc.CONSUMER_KEY, tc.CONSUMER_KEY_SECRET)
		self.auth.set_access_token(tc.ACCESS_TOKEN, tc.ACCESS_TOKEN_SECRET)

		self.connection = MongoClient(tc.DB_HOST, tc.DB_PORT)
		self.db = self.connection[tc.DB_NAME]
		self.db.authenticate(tc.DB_USER, tc.DB_PASS)
		self.clear_tables()

	def stream_tweets(self, keywords):
		listener = TwitOutListener(self.db)

		stream = Stream(self.auth, listener)

		stream.filter(track=keywords, async=True)

		minute = 1
		try:
			while(True):
				time.sleep(60)
				self.print_data(minute)
				minute += 1
		except KeyboardInterrupt:
			print("Terminating Stream")
			stream.disconnect()

	def clear_tables(self):
		self.db.tweet_count.remove({})
		self.db.link_url.remove({})
		self.db.failed_urls.remove({})

	def print_data(self, minute):
		print("Data since {0} minutes".format(minute))

		self.print_user_data()
		print("-"*50)
		self.print_link_data()
		print("-"*50)

	def print_user_data(self):

		user_report = PrettyTable(['User', "Tweets"])

		for row in self.db.tweet_count.find():
			user_report.add_row([row["user"], row["tweets"]])

		print(user_report)

	def print_link_data(self):

		link_report = PrettyTable(['Domain', "Count"])
		failed_urls = self.db.failed_urls.find()
		link_count, failed_count = 0, failed_urls.count()

		for row in self.db.link_url.find().sort('count', pymongo.DESCENDING):
			link_count += row["count"]
			link_report.add_row([row["domain"], row["count"]])

		print("Total links = " + str(failed_count + link_count))
		print("Failed to resolve links = " + str(failed_urls.count()))
		print(link_report)

class TwitOutListener(StreamListener):

	def __init__(self, db):
		self.db = db

	def on_data(self, data):
		data = json.loads(data)

		self.store_user_report_data(data)
		self.parse_links_report_data(data)

		return True

	def on_error(self, status):
		print(status)
		return False

	def store_user_report_data(self, data):

		exists = self.db.tweet_count.find_one({
			'user': data['user']['screen_name']
		})

		if exists:
			self.db.tweet_count.update_one(
				{'tweets': data['user']['screen_name']},
				{'$set': {'tweets': data['user']['statuses_count']}},
				upsert=False
			)
		else:
			self.db.tweet_count.insert({
				'user': data['user']['screen_name'],
				'tweets': data['user']['statuses_count']
			})

	def parse_links_report_data(self, data):
		# read the tweet data
		text = data["text"].encode("utf-8")

		# find url from the tweet data
		urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)

		for url in urls:

			try:
				req = liburl.Request(url, headers={'User-Agent' : "Magic Browser"})
				original_url = liburl.urlopen(req).geturl()
				extract = tldextract.extract(original_url)
				self.store_links_report_data(original_url, extract)
			except:
				original_url = url
				self.db.failed_urls.insert({'url': url})

	def store_links_report_data(self, original_url, extract):

		exists = self.db.link_url.find_one({
			'domain': extract.domain
		})

		if exists:
			self.db.link_url.update_one(
				{'domain': extract.domain},
				{'$set': {'count': exists["count"]+1}},
				upsert=False
			)
		else:
			self.db.link_url.insert({
				'url': original_url,
				'domain': extract.domain,
				'count': 1
			})

if __name__ == "__main__":

	streamer = TwitterStreamer()

	# python 2 and 3 supported
	try:
		keyword = raw_input("Please enter the keyword you want to track : ")
	except NameError:
		keyword = input("Please enter the keyword you want to track : ")

	streamer.stream_tweets([keyword])
