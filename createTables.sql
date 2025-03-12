-- Disable commits and foreign key checks to minimize import errors.  These are set back at the end of DDL import.
SET FOREIGN_KEY_CHECKS=0;
SET AUTOCOMMIT=0;

-- Drop existing tables to re-create tables
DROP TABLE IF EXISTS WeatherForecasts;

/*-------------------------------------------
*               CREATE tables
*/-------------------------------------------

-- WeatherForecasts table contains weather data
CREATE TABLE WeatherForecasts (
    weatherForecastId int(11) NOT NULL AUTO_INCREMENT,
    date datetime NOT NULL,
    temperature_2m float(11) NOT NULL,
    humidity_2m float(11) NOT NULL,
    rain float(11) NOT NULL,
    weather_code float(11) NOT NULL,
    cloud_cover float(11) NOT NULL,
    areaName varchar(50) NOT NULL,
    PRIMARY KEY (weatherForecastId)
);

-- Enable commit and foreign key checks to catch errors
SET FOREIGN_KEY_CHECKS=1;
COMMIT;
