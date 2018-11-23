from tweepy import OAuthHandler, Stream
from streamer_listener import StreamerListener
from pymongo import MongoClient
from prettytable import PrettyTable
from datetime import datetime, timedelta

import twitter_credentials as creds, time, pymongo


class TwitterStreamer():
	"""
		Establish connection with database
		Establish conncetion with Stream API
		Print generated reports every minute
	"""

	def __init__(self):
		''' set up connection variables and clear previos data from db tables '''

		self.auth = OAuthHandler(creds.CONSUMER_KEY, creds.CONSUMER_KEY_SECRET)
		self.auth.set_access_token(creds.ACCESS_TOKEN, creds.ACCESS_TOKEN_SECRET)

		self.connection = MongoClient(creds.DB_HOST, creds.DB_PORT)
		self.db = self.connection[creds.DB_NAME]
		self.db.authenticate(creds.DB_USER, creds.DB_PASS)
		self.time_diff = datetime.now()
		self.clear_tables({})

	def stream_tweets(self, keywords):
		'''
			Start streaming data asynchronously.
			Print reports every 1 minute
			Press CTRL+C to terminate stream 
		'''

		listener = StreamerListener(self.db)

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

	def clear_tables(self, condition):
		''' Clears out data from tables '''
		self.db.tweet_count.remove(condition)
		self.db.link_url.remove(condition)
		self.db.failed_urls.remove(condition)
		self.db.words.remove(condition)

	def print_data(self, minute):
		print("Data since last 5 minutes")

		self.time_diff = datetime.now() - timedelta(minutes=5)
		self.clear_tables({"timestamp": {"$lt": self.time_diff}})

		self.print_user_data()
		print("-"*50)
		self.print_link_data()
		print("-"*50)
		self.print_words_data()
		print("-"*50)

	def print_user_data(self):
		''' Prints User and their Tweet count '''

		user_report = PrettyTable(['User', "Tweets"])

		data = self.db.tweet_count.aggregate([
			{"$match": {"timestamp": {"$gt": self.time_diff}}},
			{"$group":{"_id":"$user", "sum":{"$sum":1}}}
		])

		for row in data:
			user_report.add_row([row["_id"], row["sum"]])

		print(user_report)

	def print_link_data(self):
		'''
			Prints total links received from incoming streaming data [ working as well as failed ]
			Print total number of links that couldn't be resolved.
			Prints a table displaying domain of the links with their count.
		'''

		link_report = PrettyTable(['Domain', "Count"])
		failed_count = self.db.failed_urls.find({"timestamp": {"$gt": self.time_diff}}).count()
		link_count = 0

		data = self.db.link_url.aggregate([
			{"$match": {"timestamp": {"$gt": self.time_diff}}},
			{"$group":{"_id":"$domain", "sum":{"$sum":1}}},
			{"$sort": {"sum": -1}}, {"$limit": 10}
		])

		for row in data:
			link_count += row["sum"]
			link_report.add_row([row["_id"], row["sum"]])

		print("Total links = " + str(failed_count + link_count))
		print("Failed to resolve links = " + str(failed_count))
		print(link_report)

	def print_words_data(self):
		''' Prints table containing unique words and their count '''

		words_report = PrettyTable(['Words', "Count"])

		data = self.db.words.aggregate([
			{"$match": {"timestamp": {"$gt": self.time_diff}}},
			{"$group":{"_id":"$word", "sum":{"$sum":1}}},
			{"$sort": {"sum": -1}}, {"$limit": 10}
		])

		for row in data:
			words_report.add_row([row["_id"], row["sum"]])

		print(words_report)

if __name__ == "__main__":

	streamer = TwitterStreamer()

	# python 2 and 3 supported
	try:
		keyword = raw_input("Please enter the keyword you want to track : ")
	except NameError:
		keyword = input("Please enter the keyword you want to track : ")

	print("Please wait while the report is being generated.")
	print("The reports will be printed every 1 minute.")

	streamer.stream_tweets([keyword])
