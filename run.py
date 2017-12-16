# AUTHOR: Kale Miller
# DESCRIPTION: Collects the weather data in WA.

from bs4 import BeautifulSoup
import urllib2, os, zipfile, glob, pandas


def fetchStationList():
    """Extracts the station list from the csv file."""
    return None


def downloadDataForStation(n):
    """Fetches the raw zipped file from BOM to process."""
    return None


def unzip(zip_ref):
    """Unzippes the reference specified into a new directory."""
    return None

def importStationData(csv_file):
    "Given the csv file, it will import and clean the raw data."
    return None


def formatMultiIndexDataframe(dataframes_dict):
    """By passing in the dictionary, a multi-indexed dataframe will be created."""
    return None


def main():
    """Runs the main script."""
    BOM_HOME = r'http://www.bom.gov.au'

    station_list = fetchStationList()
    dataframes = dict()
    for n in station_list:
        downloadDataForStation(n)
        unzip('station_%s.zip' % n)
        dataframes[n] = importStationData(glob.glob('station_%s/*.csv' % n)[0])
    export_df = formatMultiIndexDataframe(dataframes)
    export_df.to_csv('weatherstations_WA.csv')
    print "Executed successfully."
    return


if __name__ == '__main__':
    main()
