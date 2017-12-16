# AUTHOR: Kale Miller
# DESCRIPTION: Collects the weather data in WA.

from bs4 import BeautifulSoup
import pandas as pd
import urllib2, os, zipfile, glob


def fetchStationList(station_csv_name):
    """Extracts the station list from the csv file."""
    station_df = pd.read_csv(station_csv_name)
    return station_df['Site']


def downloadDataForStation(n, datadir='./', verbose=False, skipexistingdata=False):
    """Fetches the raw zipped file from BOM to process."""

    # If the data already exists, possibly skip over it.
    if ('station_%s.zip' % n in os.listdir(datadir)) and skipexistingdata:
        if verbose: print "Skipped station %s" % n
        return None

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


def importRainfallData(csv_file):
    "Given the rainfall csv file, it will import and clean the raw data."
    df = pd.read_csv(csv_file)

    # Rename the columns
    df.rename(columns={
        'Bureau of Meteorology station number': "Station Number",
        'Rainfall amount (millimetres)': 'Rainfall',
    }, inplace=True)

    # Combine date into a single column.
    df['Year'] = df['Year'].map(str)
    df['Month'] = df['Month'].map(lambda x: str(x).zfill(2))
    df['Day'] = df['Day'].map(lambda x: str(x).zfill(2))
    df.insert(
        2, 'Date',
        df['Year'] + '-' +
        df['Month'] + '-' +
        df['Day']
    )
    df.drop(['Year', 'Month', 'Day'], axis=1, inplace=True)

    # Next, drop the first and second columns.
    df.drop(["Product code", "Station Number"], axis=1, inplace=True)

    # Drop any rows that are before the start of data collection (i.e. drop Jan if started in Feb).
    data_start = df['Rainfall'].first_valid_index()
    data_finish = df['Rainfall'].last_valid_index()
    df = df[data_start:data_finish]

    # Set the date as the index column.
    df.set_index('Date', inplace=True)

    # Finally return the result.
    return df


def formatMultiIndexDataframe(dataframes_dict):
    """By passing in the dictionary, a multi-indexed dataframe will be created."""
    stations, dfs = zip(*dataframes_dict.items())
    return pd.concat(dfs, keys=stations, names=['Station Number', 'Date'])


def tidyUp(datadir='./'):
    """Goes through and cleans up after itself."""
    for f in glob.glob(datadir+'*.zip'):
        os.remove(f)
    return


def main():
    """Runs the main script."""
    BOM_HOME = r'http://www.bom.gov.au'
    WEATHERSTATIONS_CSV = 'weather_stations.csv'
    DATA_DIRECTORY = 'Data/'
    EXPORT_NAME = 'weatherstations_WA.csv'

    station_list = fetchStationList(WEATHERSTATIONS_CSV)
    dataframes = dict(); total_rows = len(station_list)
    for ii, n in station_list.iteritems():
        print "Station %s/%s" % (ii+1, total_rows)
        downloadDataForStation(n, datadir=DATA_DIRECTORY, skipexistingdata=True)
        unzip(DATA_DIRECTORY+'station_%s.zip' % n)
        dataframes[n] = importRainfallData(glob.glob(DATA_DIRECTORY+'station_%s/*.csv' % n)[0])
    export_df = formatMultiIndexDataframe(dataframes)
    export_df.to_csv(EXPORT_NAME)
    tidyUp(datadir=DATA_DIRECTORY)
    print "Executed successfully."
    return


if __name__ == '__main__':
    main()
