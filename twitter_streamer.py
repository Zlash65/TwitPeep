from tweepy import OAuthHandler, Stream
from streamer_listener import StreamerListener
from pymongo import MongoClient
from prettytable import PrettyTable

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
		self.clear_tables()

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

	def clear_tables(self):
		''' Clears out data from tables '''
		self.db.tweet_count.remove({})
		self.db.link_url.remove({})
		self.db.failed_urls.remove({})
		self.db.words.remove({})

	def print_data(self, minute):
		print("Data since {0} minutes".format(minute))

		self.print_user_data()
		print("-"*50)
		self.print_link_data()
		print("-"*50)
		self.print_words_data()
		print("-"*50)

	def print_user_data(self):
		''' Prints User and their Tweet count '''

		user_report = PrettyTable(['User', "Tweets"])

		for row in self.db.tweet_count.find():
			user_report.add_row([row["user"], row["tweets"]])

		print(user_report)

	def print_link_data(self):
		'''
			Prints total links received from incoming streaming data [ working as well as failed ]
			Print total number of links that couldn't be resolved.
			Prints a table displaying domain of the links with their count.
		'''

		link_report = PrettyTable(['Domain', "Count"])
		failed_urls = self.db.failed_urls.find()
		link_count, failed_count = 0, failed_urls.count()

		for row in self.db.link_url.find().sort('count', pymongo.DESCENDING):
			link_count += row["count"]
			link_report.add_row([row["domain"], row["count"]])

		print("Total links = " + str(failed_count + link_count))
		print("Failed to resolve links = " + str(failed_urls.count()))
		print(link_report)

	def print_words_data(self):
		''' Prints table containing unique words and their count '''

		words_report = PrettyTable(['Words', "Count"])

		for row in self.db.words.find().sort('count', pymongo.DESCENDING).limit(10):
			words_report.add_row([row["word"], row["count"]])

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
