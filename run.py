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
        self.directory = self.datadir+self.saveformat
        return None

    def _initaliseUniqueVariables(self, downloadFileName, pageCode, downloadContainerTitle):
        """Most of the process is identical: just set the vars that are unique."""
        self.webpage = (
            r'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=%s&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=' % pageCode
            + str(self.n).zfill(6)
        )
        self.HTML_download_attribute = {'title': downloadContainerTitle}
        self.fname = downloadFileName
        return None

    def downloadZippedData(self, skipexisting=True):
        """Downloads the zipped data for the given station."""
        # Check to see if the file is already downloaded.
        if glob.glob(self.directory+'.zip') and skipexisting:
            return None

        # First get the download link.
        page = urllib2.urlopen(self.webpage)
        soup = BeautifulSoup(page, 'html.parser')
        download_link_extension = soup.find(
            'a', self.HTML_download_attribute
        )['href']

        # Then we can download it.
        data = urllib2.urlopen(str(self.HOME + download_link_extension))
        with open(self.directory+'.zip', 'wb') as f:
            f.write(data.read())
        return None

    def unzipIt(self):
        """Unzips the zipped file."""
        zip_ref = self.directory+'.zip'
        basename = os.path.splitext(zip_ref)[0]
        with zipfile.ZipFile(zip_ref, "r") as z:
            z.extractall(basename)
        return None

    def tidyStationDir(self):
        """Renames the CSV and TXT file so it is easily identifiable."""
        for f in os.listdir(self.directory):
            fname, fext = os.path.splitext(f)
            old, new = self.directory+'/'+f, self.directory+'/'+self.fname+fext
            os.rename(old, new)
        return None

    def _importIt(self, renameDict):
        """Imports (and tidies) in the data."""
        self.df = pd.read_csv(self.directory+'/%s.csv'%self.fname)

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

    def importIt(self):
        """Public version of the _importIt method."""
        return None  # Simply namespace placeholder for other methods.

    def autorun(self):
        """Automatically run the entire data extraction process."""
        self.downloadZippedData()
        self.unzipIt()
        self.tidyStationDir()
        self.importIt()
        return None


class RainfallWeatherStation(WeatherStation):
    """The class that handles fetching the data for the rainfall."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(RainfallWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            downloadFileName='rainfall',
            pageCode=136,
            downloadContainerTitle='Data file for daily rainfall data for all years'
        )
        if autorun: self.autorun()
        return None

    def importIt(self):
        """The importIt method for the rainfall data."""
        self._importIt(
            renameDict={
                'Bureau of Meteorology station number': "Station Number",
                'Rainfall amount (millimetres)': 'Rainfall',
            }
        )
        return None


class MaxTempWeatherStation(WeatherStation):
    """The class that handles fetching the data for the maximum temperature."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(MaxTempWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            downloadFileName='max_temperature',
            pageCode=122,
            downloadContainerTitle="Data file for daily maximum temperature data for all years"
        )
        if autorun: self.autorun()
        return None

    def importIt(self):
        """The importIt method for the temperature data."""
        self._importIt(
            renameDict={
                'Bureau of Meteorology station number': "Station Number",
                'Maximum temperature (Degree C)': 'Maximum Temperature',
            }
        )
        return None


class MinTempWeatherStation(WeatherStation):
    """The class that handles fetching the data for the minimum temperature."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(MinTempWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            downloadFileName='min_temperature',
            pageCode=123,
            downloadContainerTitle="Data file for daily minimum temperature data for all years"
        )
        if autorun: self.autorun()
        return None

    def importIt(self):
        """The importIt method for the temperature data."""
        self._importIt(
            renameDict={
                'Bureau of Meteorology station number': "Station Number",
                'Minimum temperature (Degree C)': 'Minimum Temperature',
            }
        )
        return None


class SolarExposureWeatherStation(WeatherStation):
    """The class that handles fetching the data for the solar exposure."""

    def __init__(self, n, autorun=True):
        """Initialise the class."""
        super(MinTempWeatherStation, self).__init__(n)
        self._initaliseUniqueVariables(
            downloadFileName='solar_exposure',
            pageCode=193,
            downloadContainerTitle="Data file for daily solar exposure data for all years"
        )
        if autorun: self.autorun()
        return None

    def importIt(self):
        """The importIt method for the temperature data."""
        self._importIt(
            renameDict={
                'Bureau of Meteorology station number': "Station Number",
                'Daily global solar exposure (MJ/m*m)': 'Solar Exposure',
            }
        )
        return None


def main(import_csv, data_dir, export_csv, debug=False):
    """Runs the main script."""
    # Fetch station list.
    station_list = fetchStationList(import_csv)
    if debug: station_list = station_list[:10]

    # Extract the data from that list.
    dataframes = dict(); total_rows = len(station_list)
    for ii, n in station_list.iteritems():
        print "Station %s/%s" % (ii+1, total_rows)
        station = RainfallWeatherStation(n)
        dataframes[n] = station.df

    # Format export csv.
    export_df = formatMultiIndexDataframe(dataframes)
    if debug: export = 'debug.csv'
    export_df.to_csv(export_csv)

    # Tidy up and print.
    if not debug: tidyUp(datadir=data_dir)
    print "Executed successfully."
    return


if __name__ == '__main__':
    WEATHERSTATIONS_CSV = 'ExploratoryData/weather_stations.csv'
    DATA_DIRECTORY = 'Data/'
    EXPORT_NAME = 'weatherstations_export_WA.csv'
    DEBUG = False

    main(WEATHERSTATIONS_CSV, DATA_DIRECTORY, EXPORT_NAME, DEBUG)
