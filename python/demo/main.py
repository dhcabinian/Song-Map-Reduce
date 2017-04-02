#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" This is a sample application that tests the MapReduce API.

It does so by allowing users to upload a zip file containing plaintext files
and perform some kind of analysis upon it. Currently three types of MapReduce
jobs can be run over user-supplied input data: a WordCount MR that reports the
number of occurrences of each word, an Index MR that reports which file(s) each
word in the input corpus comes from, and a Phrase MR that finds statistically
improbably phrases for a given input file (this requires many input files in the
zip file to attain higher accuracies)."""

__author__ = """aizatsky@google.com (Mike Aizatsky), cbunch@google.com (Chris
Bunch)"""

# Using opensource naming conventions, pylint: disable=g-bad-name

import datetime
import jinja2
import logging
import re
import urllib
import webapp2

from google.appengine.ext import blobstore
from google.appengine.ext import db

from google.appengine.ext.webapp import blobstore_handlers

from google.appengine.api import app_identity
from google.appengine.api import taskqueue
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler


class FileMetadata(db.Model):
	"""A helper class that will hold metadata for the user's blobs.

	Specifially, we want to keep track of who uploaded it, where they uploaded it
	from (right now they can only upload from their computer, but in the future
	urlfetch would be nice to add), and links to the results of their MR jobs. To
	enable our querying to scan over our input data, we store keys in the form
	'user/date/blob_key', where 'user' is the given user's e-mail address, 'date'
	is the date and time that they uploaded the item on, and 'blob_key'
	indicates the location in the Blobstore that the item can be found at. '/'
	is not the actual separator between these values - we use '..' since it is
	an illegal set of characters for an e-mail address to contain.
	"""

	__SEP = ".."
	__NEXT = "./"

	owner = db.UserProperty()
	filename = db.StringProperty()
	uploadedOn = db.DateTimeProperty()
	source = db.StringProperty()
	blobkey = db.StringProperty()
	song_sales_link = db.StringProperty()
	song_profit_link = db.StringProperty()
	artist_songs_link = db.StringProperty()
	artist_profit_link = db.StringProperty()



	@staticmethod
	def getFirstKeyForUser(username):
		"""Helper function that returns the first possible key a user could own.

		This is useful for table scanning, in conjunction with getLastKeyForUser.

		Args:
			username: The given user's e-mail address.
		Returns:
			The internal key representing the earliest possible key that a user could
			own (although the value of this key is not able to be used for actual
			user data).
		"""

		return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

	@staticmethod
	def getLastKeyForUser(username):
		"""Helper function that returns the last possible key a user could own.

		This is useful for table scanning, in conjunction with getFirstKeyForUser.

		Args:
			username: The given user's e-mail address.
		Returns:
			The internal key representing the last possible key that a user could
			own (although the value of this key is not able to be used for actual
			user data).
		"""

		return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

	@staticmethod
	def getKeyName(username, date, blob_key):
		"""Returns the internal key for a particular item in the database.

		Our items are stored with keys of the form 'user/date/blob_key' ('/' is
		not the real separator, but __SEP is).

		Args:
			username: The given user's e-mail address.
			date: A datetime object representing the date and time that an input
				file was uploaded to this app.
			blob_key: The blob key corresponding to the location of the input file
				in the Blobstore.
		Returns:
			The internal key for the item specified by (username, date, blob_key).
		"""

		sep = FileMetadata.__SEP
		return str(username + sep + str(date) + sep + blob_key)


class IndexHandler(webapp2.RequestHandler):
	"""The main page that users will interact with, which presents users with
	the ability to upload new data or run MapReduce jobs on their existing data.
	"""

	template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
																		autoescape=True)

	def get(self):
		user = users.get_current_user()
		username = user.nickname()

		first = FileMetadata.getFirstKeyForUser(username)
		last = FileMetadata.getLastKeyForUser(username)

		q = FileMetadata.all()
		q.filter("__key__ >", first)
		q.filter("__key__ < ", last)
		results = q.fetch(10)

		items = [result for result in results]
		length = len(items)

		print "Length of items: "
		print length

		print "\nItems:"
		print items

		bucket_name = app_identity.get_default_gcs_bucket_name()
		upload_url = blobstore.create_upload_url("/upload",gs_bucket_name=bucket_name)

		self.response.out.write(self.template_env.get_template("index.html").render(
				{"username": username,
				 "items": items,
				 "length": length,
				 "upload_url": upload_url}))

	def post(self):
		filekey = self.request.get("filekey")
		blob_key = self.request.get("blobkey")

		if self.request.get("song_sales"):
			pipeline = SongSalesPipeline(filekey, blob_key)
		elif self.request.get("song_profit"):
			pipeline = SongProfitPipeline(filekey, blob_key)
		elif self.request.get("artist_songs"):
			pipeline = ArtistSongsPipeline(filekey, blob_key)
		elif self.request.get("artist_profit"):
			pipeline = ArtistProfitPipeline(filekey, blob_key)

		pipeline.start()
		self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

def split_into_purchase(purchases):
	"""Split a text into list of purchases."""
	if re.search('[a-zA-Z0-9]', purchases):
		   return purchases.split("\n")
	else:
		return None

def split_into_data(purchase):
	"""Split a data entry of purchases into data"""
	purchase_formatted = re.sub(r'\r\n',"",purchase)
	components = purchase_formatted.split("\t")
	if component_checker(components):
		date_time = components[0]
		user = components[1]
		title = components[2]
		artist = components[3]
		if len(components) is 7:
			album = ""
			genre = components[4]
			cost = components[6]
		elif len(components) is 8:
			album = components[4]
			genre = components[5]
			cost = components[7]
		else:
			print "Purchase of incorrect size"
			return
		return [date_time, user, title, artist, album, genre, cost]
	else:
		return None

def component_checker(components):
	if (len(components) == 7 or len(components) == 8):
		if (re.search('[0-9]', components[0])):
			return True
	return False

def song_sales_map(data):
	"""
	Count the number of songs sold for each song that has been purchased at least once and order the final result from largest to smallest count. For a song to be considered the same, the song title, artist name, and album title must all match.
	"""
	(entry, text_fn) = data
	text = text_fn()
	logging.debug("Song_sales_map got %s", text)
	purchases = split_into_purchase(text)
	if purchases is not None:
		for p in purchases:
			purchase = split_into_data(p)
			if purchase is not None:
				song = purchase[2] + "," + purchase[3] + "," + purchase[4]
				yield(song, "")

def song_sales_reduce(key, values):
	yield "%s; %d\n" % (key, len(values))

def song_profit_map(data):
	"""
	Calculate the total dollar amount of sales for each song and order the final result from largest to smallest amount. The same song matching rule applies as in Item 1.
	"""
	(entry, text_fn) = data
	text = text_fn()
	logging.debug("Song_profit_map got %s", text)
	purchases = split_into_purchase(text)
	if purchases is not None:
		for p in purchases:
			purchase = split_into_data(p)
			if purchase is not None:
				song = purchase[2] + "," + purchase[3] + "," + purchase[4]
				yield(song, float(purchase[6]))

def song_profit_reduce(key, values):
	total_song_profit = 0.0;
	for value in values:
		total_song_profit += float(value)
	yield "%s; %f\n" % (key, total_song_profit)

def artist_songs_map(data):
	"""
	Count the number of songs sold for each artist that has had at least one song purchased and order the final result from largest to smallest count. Here, only the artist name field must match.
	"""
	(entry, text_fn) = data
	text = text_fn()
	logging.debug("Song_profit_map got %s", text)
	purchases = split_into_purchase(text)
	if purchases is not None:
		for p in purchases:
			purchase = split_into_data(p)
			if purchase is not None:
				yield(purchase[3], "")

def artist_songs_reduce(key, values):
	yield "%s; %d\n" % (key, len(values))

def artist_profit_map(data):
	"""
	Calculate the total dollar amount of sales for each artist and order the final result from largest to smallest amount. Again, only the artist name must match.
	"""
	(entry, text_fn) = data
	text = text_fn()
	logging.debug("Song_profit_map got %s", text)
	purchases = split_into_purchase(text)
	if purchases is not None:
		for p in purchases:
			purchase = split_into_data(p)
			if purchase is not None:
				song = purchase[2] + "," + purchase[3] + "," + purchase[4]
				yield(purchase[3], float(purchase[6]))

def artist_profit_reduce(key, values):
	total_song_profit = 0.0;
	for value in values:
		total_song_profit += float(value)
	yield "%s; %f\n" % (key, total_song_profit)

class SongSalesPipeline(base_handler.PipelineBase):
	"""A pipeline to run song sales count.

	Args:
		blobkey: blobkey to process as string. Should be a zip archive with
			text files inside.
	"""

	def run(self, filekey, blobkey):
		logging.debug("filename is %s" % filekey)
		bucket_name = app_identity.get_default_gcs_bucket_name()
		output = yield mapreduce_pipeline.MapreducePipeline(
				"song_sales",
				"main.song_sales_map",
				"main.song_sales_reduce",
				"mapreduce.input_readers.BlobstoreZipInputReader",
				"mapreduce.output_writers.GoogleCloudStorageOutputWriter",
				mapper_params={
						"blob_key": blobkey,
				},
				reducer_params={
						"output_writer": {
								"bucket_name": bucket_name,
								"content_type": "text/plain",
						}
				},
				shards=1)
		yield StoreOutput("song_sales", filekey, output)

class SongProfitPipeline(base_handler.PipelineBase):
	"""A pipeline to run song profit

	Args:
		blobkey: blobkey to process as string. Should be a zip archive with
			text files inside.
	"""


	def run(self, filekey, blobkey):
		bucket_name = app_identity.get_default_gcs_bucket_name()
		output = yield mapreduce_pipeline.MapreducePipeline(
				"song_profit",
				"main.song_profit_map",
				"main.song_profit_reduce",
				"mapreduce.input_readers.BlobstoreZipInputReader",
				"mapreduce.output_writers.GoogleCloudStorageOutputWriter",
				mapper_params={
						"blob_key": blobkey,
				},
				reducer_params={
						"output_writer": {
								"bucket_name": bucket_name,
								"content_type": "text/plain",
						}
				},
				shards=1)
		yield StoreOutput("song_profit", filekey, output)

class ArtistSongsPipeline(base_handler.PipelineBase):
	"""A pipeline to run artist songs

	Args:
		blobkey: blobkey to process as string. Should be a zip archive with
			text files inside.
	"""


	def run(self, filekey, blobkey):
		bucket_name = app_identity.get_default_gcs_bucket_name()
		output = yield mapreduce_pipeline.MapreducePipeline(
				"artist_songs",
				"main.artist_songs_map",
				"main.artist_songs_reduce",
				"mapreduce.input_readers.BlobstoreZipInputReader",
				"mapreduce.output_writers.GoogleCloudStorageOutputWriter",
				mapper_params={
						"blob_key": blobkey,
				},
				reducer_params={
						"output_writer": {
								"bucket_name": bucket_name,
								"content_type": "text/plain",
						}
				},
				shards=1)
		yield StoreOutput("artist_songs", filekey, output)

class ArtistProfitPipeline(base_handler.PipelineBase):
	"""A pipeline to run Index demo.

	Args:
		blobkey: blobkey to process as string. Should be a zip archive with
			text files inside.
	"""


	def run(self, filekey, blobkey):
		bucket_name = app_identity.get_default_gcs_bucket_name()
		output = yield mapreduce_pipeline.MapreducePipeline(
				"artist_profit",
				"main.artist_profit_map",
				"main.artist_profit_reduce",
				"mapreduce.input_readers.BlobstoreZipInputReader",
				"mapreduce.output_writers.GoogleCloudStorageOutputWriter",
				mapper_params={
						"blob_key": blobkey,
				},
				reducer_params={
						"output_writer": {
								"bucket_name": bucket_name,
								"content_type": "text/plain",
						}
				},
				shards=1)
		yield StoreOutput("artist_profit", filekey, output)

class StoreOutput(base_handler.PipelineBase):
	"""A pipeline to store the result of the MapReduce job in the database.

	Args:
		mr_type: the type of mapreduce job run (e.g., WordCount, Index)
		encoded_key: the DB key corresponding to the metadata of this job
		output: the gcs file path where the output of the job is stored
	"""

	def run(self, mr_type, encoded_key, output):
		logging.debug("output is %s" % str(output))
		key = db.Key(encoded=encoded_key)
		m = FileMetadata.get(key)

		blobstore_filename = "/gs" + output[0]
		blobstore_gs_key = blobstore.create_gs_key(blobstore_filename)
		url_path = "/blobstore/" + blobstore_gs_key

		if mr_type == "song_sales":
			m.song_sales_link = url_path
		elif mr_type == "song_profit":
			m.song_profit_link = url_path
		elif mr_type == "artist_songs":
			m.artist_songs_link = url_path
		elif mr_type == "artist_profit":
			m.artist_profit_link = url_path
		m.put()

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
	"""Handler to upload data to blobstore."""

	def post(self):
		source = "uploaded by user"
		upload_files = self.get_uploads("file")
		blob_key = upload_files[0].key()
		name = self.request.get("name")

		user = users.get_current_user()

		username = user.nickname()
		date = datetime.datetime.now()
		str_blob_key = str(blob_key)
		key = FileMetadata.getKeyName(username, date, str_blob_key)

		m = FileMetadata(key_name = key)
		m.owner = user
		m.filename = name
		m.uploadedOn = date
		m.source = source
		m.blobkey = str_blob_key
		m.put()

		self.redirect("/")

class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
	"""Handler to download blob by blobkey."""

	def get(self, key):
		key = str(urllib.unquote(key)).strip()
		logging.debug("key is %s" % key)
		self.send_blob(key)


app = webapp2.WSGIApplication(
		[
				('/', IndexHandler),
				('/upload', UploadHandler),
				(r'/blobstore/(.*)', DownloadHandler),
		],
		debug=True)
