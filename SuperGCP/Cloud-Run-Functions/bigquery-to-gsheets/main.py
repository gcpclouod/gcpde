import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
from google.cloud import bigquery 
from google.cloud import secretmanager
import functions_framework 
import json

@functions_framework.http 
def bq_to_gsheets_cloud_func(request):
	# two apis should be enabled google drive and sheets
	scopes = ['https://www.googleapis.com/auth/spreadsheets',
			'https://www.googleapis.com/auth/drive']
	#credentials = Credentials.from_service_account_file("/home/saisrikanth1918/Scecrets/kinetic-calling-343310-dd3f9d78c47d.json", scopes=scopes)
	# Get the path to the service account file from an environment variable
	
	client = secretmanager.SecretManagerServiceClient()
	name = "projects/840233991363/secrets/bqtogsheets/versions/1"
	response = client.access_secret_version(name=name)
	payload = response.payload.data.decode("UTF-8")
	service_account_info = json.loads(payload)	

	# Create credentials using the service account file
	credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)

	# Use the credentials to authenticate with Google Cloud services, such as BigQuery
	gc = gspread.authorize(credentials)
	your_sheet_key = "1LQi7CAc_oaoiFTQMTbGF19iaOLzpdWsbukBWUZ-kptM"
	gauth = GoogleAuth()
	drive = GoogleDrive(gauth)	
	# open a google sheet
	gs = gc.open_by_key(your_sheet_key)
	# select a work sheet from its name
	worksheet1 = gs.worksheet('Sheet1')


	# dataframe (create or import it)
	client = bigquery.Client()	
	# Defines the SQL query to be executed
	query = """
		SELECT *  FROM woven-name-434311-i8.cloudfunction_demo.cloudfunctiondemotable;
	"""	
	# Executes the SQL query and converts the results to a Pandas DataFrame
	df = client.query(query).to_dataframe()


	# write to dataframe
	worksheet1.clear()
	set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
	include_column_header=True, resize=True)

	# http response
	return 'Data successfully inserted from Google Sheets to BigQuery'