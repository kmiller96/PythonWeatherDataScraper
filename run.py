# AUTHOR: Kale Miller
# DESCRIPTION: Collects the weather data in WA.

from bs4 import BeautifulSoup
import pandas as pd
import urllib2, os, zipfile, glob, shutil

###########################
# DEFINE GLOBAL FUNCTIONS #
###########################

def fetchStationList(station_csv_name):
    """Extracts the station list from the csv file."""
    station_df = pd.read_csv(station_csv_name)
    return station_df['Site']

def tidyUp(datadir='./'):
    """Goes through and cleans up after itself."""
    for f in glob.glob(datadir+'*.zip'):
        os.remove(f)
    for f in glob.glob(datadir+'station*'):
        shutil.rmtree(f)
    return

def formatMultiIndexDataframe(dataframes_dict):
    """By passing in a dictionary of dataframes, a multi-indexed dataframe will be created."""
    stations, dfs = zip(*dataframes_dict.items())
    return pd.concat(dfs, keys=stations, names=['Station Number', 'Date'])

##################################
# CREATE DATA PROCESSING CLASSES #
##################################

class WeatherStation(object):
    """The parent class to all weather station's data collection classes."""
    HOME = r'http://www.bom.gov.au'

    def __init__(self, n):
        """Initialise the station's number."""
        # Initialise the variables.
        self.n = n
        self.webpage = ''  # NOTE: This must be defined in the children classes.
        self.HTML_download_attribute = {}  # NOTE: This must be defined in the children classes.
        self.datadir = 'Data/'
        self.saveformat = 'station_%s' % self.n
        return None

    def _initaliseUniqueVariables(self, pageCode, downloadContainerTitle):
        """Most of the process is identical: just set the vars that are unique."""
        self.webpage = (
            r'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=%s&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=' % pageCode
            + str(self.n).zfill(6)
        )
        self.HTML_download_attribute = {'title': downloadContainerTitle}
        return None

    def downloadZippedData(self):
        """Downloads the zipped data for the given station."""
        # Check to see if the file is already downloaded.
        if glob.glob(self.datadir+self.saveformat+'*'):  # Either dir or zip
            return None

        # First get the download link.
        page = urllib2.urlopen(self.webpage)
        soup = BeautifulSoup(page, 'html.parser')
        download_link_extension = soup.find(
            'a', self.HTML_download_attribute
        )['href']

        # Then we can download it.
        data = urllib2.urlopen(str(self.HOME + download_link_extension))
        with open(self.datadir+self.saveformat+'.zip', 'wb') as f:
            f.write(data.read())
        return None

    def unzipIt(self):
        """Unzips the zipped file."""
        zip_ref = self.datadir+self.saveformat+'.zip'
        basename = os.path.splitext(zip_ref)[0]
        with zipfile.ZipFile(zip_ref, "r") as z:
            z.extractall(basename)
        return None

    def _importIt(self, csv_name, renameDict):
        """Imports (and tidies) in the rainfall data."""
        self.df = pd.read_csv(glob.glob(self.datadir+self.saveformat+'/*.csv')[0])

        # Rename the columns
        self.df.rename(columns=renameDict, inplace=True)

        # Combine date into a single column.
        self.df['Year'] = self.df['Year'].map(str)
        self.df['Month'] = self.df['Month'].map(lambda x: str(x).zfill(2))
        self.df['Day'] = self.df['Day'].map(lambda x: str(x).zfill(2))
        self.df.insert(
            2, 'Date',
            self.df['Year'] + '-' +
            self.df['Month'] + '-' +
            self.df['Day']
        )
        self.df.drop(['Year', 'Month', 'Day'], axis=1, inplace=True)

        # Next, drop the first and second columns.
        self.df.drop(self.df.columns[[0, 1]], axis=1, inplace=True)

        # Drop any rows that are before the start of data collection (i.e. drop Jan if started in Feb).
        data_start = self.df[self.df.columns[1]].first_valid_index()
        data_finish = self.df[self.df.columns[1]].last_valid_index()
        self.df = self.df[data_start:data_finish]

        # Set the date as the index column.
        self.df.set_index('Date', inplace=True)
        return None


class RainfallWeatherStation(WeatherStation):
    """The class that handles fetching the data for the rainfall."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(RainfallWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            pageCode=136,
            downloadContainerTitle='Data file for daily rainfall data for all years'
        )

        # Then call the functions in order to initialise everything else.
        if autorun:
            self.downloadZippedData()
            self.unzipIt()
            self.importIt()
        return None

    def importIt(self):
        """Imports (and tidies) in the rainfall data."""
        self.df = pd.read_csv(glob.glob(self.datadir+self.saveformat+'/*.csv')[0])

        # Rename the columns
        self.df.rename(columns={
            'Bureau of Meteorology station number': "Station Number",
            'Rainfall amount (millimetres)': 'Rainfall',
        }, inplace=True)

        # Combine date into a single column.
        self.df['Year'] = self.df['Year'].map(str)
        self.df['Month'] = self.df['Month'].map(lambda x: str(x).zfill(2))
        self.df['Day'] = self.df['Day'].map(lambda x: str(x).zfill(2))
        self.df.insert(
            2, 'Date',
            self.df['Year'] + '-' +
            self.df['Month'] + '-' +
            self.df['Day']
        )
        self.df.drop(['Year', 'Month', 'Day'], axis=1, inplace=True)

        # Next, drop the first and second columns.
        self.df.drop(["Product code", "Station Number"], axis=1, inplace=True)

        # Drop any rows that are before the start of data collection (i.e. drop Jan if started in Feb).
        data_start = self.df['Rainfall'].first_valid_index()
        data_finish = self.df['Rainfall'].last_valid_index()
        self.df = self.df[data_start:data_finish]

        # Set the date as the index column.
        self.df.set_index('Date', inplace=True)
        return None


class MaxTempWeatherStation(WeatherStation):
    """The class that handles fetching the data for the maximum temperature."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(MaxTempWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            pageCode=122,
            downloadContainerTitle="Data file for daily maximum temperature data for all years"
        )

        # Then call the functions in order to initialise everything else.
        if autorun:
            self.downloadZippedData()
            self.unzipIt()
            self.importIt()
        return None


def main():
    """Runs the main script."""
    WEATHERSTATIONS_CSV = 'ExploratoryData/weather_stations.csv'
    DATA_DIRECTORY = 'Data/'
    EXPORT_NAME = 'weatherstations_export_WA.csv'

    station_list = fetchStationList(WEATHERSTATIONS_CSV)
    dataframes = dict(); total_rows = len(station_list)
    for ii, n in station_list.iteritems():
        print "Station %s/%s" % (ii+1, total_rows)
        station = RainfallWeatherStation(n)
        dataframes[n] = station.df
    export_df = formatMultiIndexDataframe(dataframes)
    export_df.to_csv(EXPORT_NAME)
    tidyUp(datadir=DATA_DIRECTORY)
    print "Executed successfully."
    return


if __name__ == '__main__':
    main()
