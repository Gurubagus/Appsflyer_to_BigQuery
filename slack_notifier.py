import os

class Error_Notifier:
	
	def main(channel, error):

		if channel == "<Slack channel name>":
			channel = "<Slack channel url>"

		message = "{\"text\": \"" + error + "\"}"
		os.system("curl -X POST -H 'Content-type: application/json' --data '" + message + "' "+ channel +"")