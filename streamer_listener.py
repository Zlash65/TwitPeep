from tweepy.streaming import StreamListener
import json, re, tldextract, string
from nltk.corpus import stopwords
from collections import Counter
from datetime import datetime

# Python 2 and 3 support
try:
	import urllib2 as liburl
except ModuleNotFoundError:
	import urllib.request as liburl

class StreamerListener(StreamListener):

	def __init__(self, db):
		''' capture connection to mongo '''
		self.db = db
		self.timestamp = datetime.now()

		# download stopwords
		try:
			s = set(stopwords.words('english'))
		except Exception as e:
			import nltk
			nltk.download('stopwords')

	def on_data(self, data):
		''' each tweet calls this method where data contains the information about the tweet '''

		data = json.loads(data)
		self.timestamp = datetime.now()

		self.store_user_report_data(data)
		self.parse_links_report_data(data)
		self.parse_words(data)

		return True

	def on_error(self, status):
		print(status)
		return False

	def store_user_report_data(self, data):
		'''
			Extract User's twitter handle and their tweet count from
			incoming streaming data.
		'''

		# exists = self.db.tweet_count.find_one({
		# 	'user': data['user']['screen_name']
		# })

		# if exists:
		# 	self.db.tweet_count.update_one(
		# 		{'tweets': data['user']['screen_name']},
		# 		{'$set': {
		# 			'tweets': exists['tweets']+1,
		# 			'timestamp': self.timestamp
		# 		}},
		# 		upsert=False
		# 	)
		# else:
		self.db.tweet_count.insert({
			'user': data['user']['screen_name'],
			'tweets': 1,
			'timestamp': self.timestamp
		})

	def parse_links_report_data(self, data):
		'''
			Parse the tweet and use regex to find link from it.
			Open the url to fetch its actual url as links can be shortened.
			Insert links that failed to be resolved maybe because at the time of tweeting
			the characters constraint truncated the link or etc.
		'''

		# extract the actual tweet
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
		'''
			Store the domain of the url.
			Also update the count of domain if it already exists.
		'''

		# exists = self.db.link_url.find_one({
		# 	'domain': extract.domain
		# })

		# if exists:
		# 	self.db.link_url.update_one(
		# 		{'domain': extract.domain},
		# 		{'$set': {
		# 			'count': exists["count"]+1,
		# 			'timestamp': self.timestamp
		# 		}},
		# 		upsert=False
		# 	)
		# else:
		self.db.link_url.insert({
			'domain': extract.domain,
			'count': 1,
			'timestamp': self.timestamp
		})

	def parse_words(self, data):
		'''
			Remove punctuation from text.
			Use Natural Language toolkit (nltk) to filter out common words.
			Use Counter function to find unique words and their count in the words list.
		'''

		text = data["text"].encode("utf-8")
		out = text.translate(string.maketrans("",""), string.punctuation)

		s = set(stopwords.words('english'))

		words = filter(lambda w: not w in s,out.split())
		words_list = Counter(words)

		for row in words_list:
			self.store_words(row, words_list[row])

	def store_words(self, word, count):
		''' store words and their count in database '''

		# exists = self.db.words.find_one({
		# 	'word': word
		# })

		# if exists:
		# 	self.db.words.update_one(
		# 		{'word': word},
		# 		{'$set': {
		# 			'count': exists["count"] + count,
		# 			'timestamp': self.timestamp
		# 		}},
		# 		upsert=False
		# 	)
		# else:
		self.db.words.insert({
			'word': word,
			'count': count,
			'timestamp': self.timestamp
		})
