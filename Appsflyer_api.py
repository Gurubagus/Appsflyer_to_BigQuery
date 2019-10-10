import requests
import os
import json
import urllib
		
class Appsflyer_API_Pull:
	# This class will send a PULL request to Appsflyer's API Endpoint to extract the chosen report
	
	def __init__(self):
		# Will initialize the BigQuery Client, script e.g. AF = Appsflyer_API_Pull()	
		
		self.api_endpoint = "https://hq.appsflyer.com/export/"  # Standard Appsflyer PULL API endpoint
		self.api_token = "<API Token>" # Enter API Token here, found under "Integration" > "API Access" in the platform
		
	def main(self, app_id, start_date, end_date, report_name):
		try:
			query_params = {
				"api_token": self.api_token,
				"from": str(start_date),
				"to": str(end_date)
				}

			file = str(app_id + "_" + report_name + "_" + start_date + "_to_" + end_date) # File name
			query_string = urllib.parse.urlencode(query_params) # Parses query params
			request_url = self.api_endpoint + app_id + "/" + report_name + "/v5?" + query_string # combines and creates request
			print(str("Making Request to Appsflyer for: " + file))
			try:
				
				with open(file,"wb") as fl: # Creates file from File name and writes API Response to it
					resp = urllib.request.urlopen(request_url) # Sends request
					fl.write(resp.read())
				print("Request Completed.")
				
			except Exception as e:
				print(e)
				
			return file # Returns file name for use by other methods
		
		except Exception as e:
			print(e)