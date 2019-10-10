import sys
import os
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, MetaData, Table, Integer, DateTime, String, Column, select, BigInteger, Numeric
from sqlalchemy.dialects import postgresql
import json
#sys.path.append("<dir/for/slack_notifier/and/or/bq_uploader/if/different>") #Directory for additional custom Classes, CHANGE TO LOCAL DIR where BigQuery/Slack Notifier is stored if seperate directory
from Appsflyer_api import Appsflyer_API_Pull 
from bq_uploader import BigQuery_uploader 
from slack_notifier import Error_Notifier

class MSTR:
	
	# This class uses custom classes from the added directory to download reports from Appsflyer and upload them to BigQuery
	
	def __init__(self):
	
		self.AF = Appsflyer_API_Pull() # Sets Appsflyer API object
		self.BQ = BigQuery_uploader() # Sets BigQuery connection
		
		self.apps = ["<list Appsflyer app ids here>", "<list Appsflyer app ids here>"] 
		self.reports = ["installs_report", "in_app_events_report","organic_in_app_events_report", "invalid_installs_report", "invalid_in_app_events_report"] # all available reports, pick and choose accordingly
		self.start_time = "00:00"
		self.end_time = "23:59"
		self.yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') # Creates datetime object for the day before date being run
		self.today = str(date.today())
		
		# Set parameters if custom date range is needed
		#self.yesterday = "e.g. 2019-09-22"
		#self.today = "e.g. 2019-09-23"
		
		self.yesterday_start = str(self.yesterday + " " + self.start_time)
		self.yesterday_end = str(self.today + " " + self.start_time)
		
		self.path = str("<dir/for/report/storage/if/desired>" + self.yesterday) # Creates path for created reports
		
		self.CONFIG = {}
		self.directory_counter = 1
		
		try:
			with open(sys.argv[1], "r") as f:
				self.CONFIG = json.load(f)
		except Exception as e:
			print('failed to read or parse config file')
			print(e)
			exit(1)
		if not self.CONFIG['user'] or not self.CONFIG['password'] or not self.CONFIG['postgres_server'] \
			or not self.CONFIG['database']:
			
			print("missing parameter in config.json. see docs.")
			exit(1)
			
		self.engine = create_engine('postgresql+psycopg2://' + self.CONFIG['user'] + ':' + self.CONFIG['password'] + 
									'@' + self.CONFIG['postgres_server'] + '/' + self.CONFIG['database'])

		self.conn = self.engine.connect()
		self.i = 0
		
	def main(self):
		
			try:
				if os.path.exists(self.path):
					for a in self.apps: # loops through all app ids listed in self.apps	(only currently for installs and in_app_events reports)
						if os.path.isfile(self.path + "/" + a + "_installs_report_"+ self.yesterday_start + "_to_" + self.yesterday_end) and not os.path.isfile(self.path + "/" + a + "_in_app_events_report_"+ self.yesterday_start + "_to_" + self.yesterday_end):
							self.reports = ["in_app_events_report"]
						elif not os.path.isfile(self.path + "/" + a + "_installs_report_"+ self.yesterday_start + "_to_" + self.yesterday_end) and os.path.isfile(self.path + "/" + a + "_in_app_events_report_"+ self.yesterday_start + "_to_" + self.yesterday_end):
							self.reports = ["installs_report"]
						elif os.path.isfile(self.path + "/" + a + "_installs_report_"+ self.yesterday_start + "_to_" + self.yesterday_end) and os.path.isfile(self.path + "/" + a + "_in_app_events_report_"+ self.yesterday_start + "_to_" + self.yesterday_end):
							print("All reports for {} for {} have already been uploaded.".format(a, self.yesterday))
							if self.directory_counter == len(self.apps):
								print("All reports for {} have been uploaded. Please choose a different date to transfer.".format(self.yesterday))
								sys.exit(1)
							self.directory_counter += 1
				else:
					os.mkdir(self.path) # Creates directory for each days reports to be stored, named with today's date
					os.chdir(self.path) # Moves current working directory to created directory

				for a in self.apps: # loops through all app ids listed in self.apps	
					for r in self.reports: # loops through all reports listed in self.reports
						f = self.AF.main(a, self.yesterday_start, self.yesterday_end, r) # Sends Pull API Request and saves report in a file in created directory
						file = str(self.path + "/" + f) # Gathers file information to direct BigQuery uploader to created report
						to_bq = self.BQ.main('Appsflyer_inbound', r, file, 'CSV') # Uploads report to appropriate table in BigQuery
						self.conn.execute("INSERT INTO <sql table name> (job_type, job_id, date, start_time, end_time, app) VALUES ('" + r + "', '"+ to_bq +"', '"+ self.yesterday +"', '"+ self.start_time + "', '"+ self.end_time +"','"+ a +"');") 

			except Exception as e:
				print(e)

			finally:
				self.conn.close()
			
			
if __name__=="__main__":
	try:
		Mstr = MSTR().main() # Initializes and runs class MSTR with main method
	except Exception as e:
		message = "Appsflyer Autoupload ERROR: " + str(e)
		error = Error_Notifier.main("error_log", message)
