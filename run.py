# AUTHOR: Kale Miller
# DESCRIPTION: Collects the weather data in WA.

from bs4 import BeautifulSoup
import pandas as pd
import urllib2, os, zipfile, glob


def fetchStationList(station_csv_name):
    """Extracts the station list from the csv file."""
    station_df = pd.read_csv(station_csv_name)
    return station_numbers = station_df['Site']


def downloadDataForStation(n, datadir='./', verbose=False):
    """Fetches the raw zipped file from BOM to process."""

    # First access the weather station's data page.
    page = urllib2.urlopen(
        r'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=136&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num='
        + str(n).zfill(6)
    )
    soup = BeautifulSoup(page, 'html.parser')

    # Then find the download link for that page.
    download_link_extension = soup.find('a',
        {'title': "Data file for daily rainfall data for all years"}
    )['href']

    # Open the download link (which is read as binary) and save it in the correct format (zip file).
    data = urllib2.urlopen(str(r'http://www.bom.gov.au' + download_link_extension))
    with open(datadir+'station_%s.zip' % n, 'wb') as zipper:
        zipper.write(data.read())

    # Finally, print a success message if verbose and return
    if verbose: print "Download for station %s was successful!" % n
    return None


def unzip(zip_ref):
    """Unzippes the reference specified into a new directory."""
    basename = os.path.splitext(zip_ref)[0]
    with zipfile.ZipFile(zip_ref, "r") as z:
        z.extractall(basename)
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
    WEATHERSTATIONS_CSV = 'weather_stations.csv'

    station_list = fetchStationList(WEATHERSTATIONS_CSV)
    dataframes = dict()
    for _, n in station_list.iteritems():
        downloadDataForStation(n)
        unzip('station_%s.zip' % n)
        dataframes[n] = importStationData(glob.glob('station_%s/*.csv' % n)[0])
    export_df = formatMultiIndexDataframe(dataframes)
    export_df.to_csv('weatherstations_WA.csv')
    print "Executed successfully."
    return


if __name__ == '__main__':
    main()
