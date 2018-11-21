from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

import twitter_credentials as tc, json, time
from prettytable import PrettyTable
from pymongo import MongoClient


class TwitterStreamer():

	def __init__(self):
		self.auth = OAuthHandler(tc.CONSUMER_KEY, tc.CONSUMER_KEY_SECRET)
		self.auth.set_access_token(tc.ACCESS_TOKEN, tc.ACCESS_TOKEN_SECRET)

		self.connection = MongoClient(tc.DB_HOST, tc.DB_PORT)
		self.db = self.connection[tc.DB_NAME]
		self.db.authenticate(tc.DB_USER, tc.DB_PASS)



	def stream_tweets(self, keywords):
		listener = TwitOutListener(self.db)

		stream = Stream(self.auth, listener)

		stream.filter(track=keywords, async=True)

		minute = 1
		try:
			while(True):
				time.sleep(60)
				self.print_user_data(minute)
				minute += 1
		except KeyboardInterrupt:
			print("Terminating Stream")
			self.db.tweet_count.remove({})
			stream.disconnect()

	def print_user_data(self, minute):

		user_report = PrettyTable(['User', "Tweets"])

		print("Data for {0} minutes".format(minute))

		for row in self.db.tweet_count.find():
			user_report.add_row([row["user"], row["tweets"]])

		print(user_report)

class TwitOutListener(StreamListener):

	def __init__(self, db):
		self.db = db

	def on_data(self, data):
		data = json.loads(data)

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

		return True
	
	def on_error(self, status):
		print(status)
		return False


if __name__ == "__main__":

	streamer = TwitterStreamer()

	# python 2 and 3 supported
	try:
		keyword = raw_input("Please enter the keyword you want to track : ")
	except NameError:
		keyword = input("Please enter the keyword you want to track : ")

	streamer.stream_tweets([keyword])
