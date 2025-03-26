-- Disable commits and foreign key checks to minimize import errors.  These are set back at the end of DDL import.
SET FOREIGN_KEY_CHECKS=0;
SET AUTOCOMMIT=0;

-- Drop existing tables to re-create tables
DROP TABLE IF EXISTS b_Crimes;
DROP TABLE IF EXISTS b_Weather;
DROP TABLE IF EXISTS s_Crimes;
DROP TABLE IF EXISTS s_Weather;

/*-------------------------------------------
*               CREATE tables
*/-------------------------------------------
-- Bronze staging table for Crimes data
CREATE TABLE b_Crimes (
    crimeId int(11) AUTO_INCREMENT,
    json_data JSON,
    last_rowNumber int(11),
    timeUploaded datetime,
    api varchar(50),
    PRIMARY KEY (crimeId)
);

-- Bronze staging table for weather data
CREATE TABLE b_Weather (
    weatherId int(11) NOT NULL AUTO_INCREMENT,
    date datetime NOT NULL,
    temperature_2m float(11) NOT NULL,
    humidity_2m float(11) NOT NULL,
    rain float(11) NOT NULL,
    weather_code float(11) NOT NULL,
    cloud_cover float(11) NOT NULL,
    areaName varchar(50) NOT NULL,
    PRIMARY KEY (weatherId)
);

-- Crimes table contains crime data
CREATE TABLE s_Crimes (
    crimeId int(11) NOT NULL AUTO_INCREMENT,
    dr_no varchar(50),
    date_rptd datetime,
    date_occ datetime,
    time_occ varchar(50),
    area varchar(50),
    area_name varchar(50),
    rpt_dist_no varchar(50),
    part_1_2 int(11),
    crm_cd varchar(50),
    crm_cd_desc varchar(500),
    mocodes varchar(50),
    vict_age int(8),
    vict_sex varchar(1),
    vict_descent varchar(50),
    premis_cd int(11),
    premis_desc varchar(100),
    weapon_used_cd varchar(100),
    weapon_desc varchar(50),
    status varchar(50),
    status_desc varchar(100),
    crm_cd_1 varchar(50),
    crm_cd_2 varchar(50),
    crm_cd_3 varchar(50),
    crm_cd_4 varchar(50),
    location varchar(50),
    cross_street varchar(50),
    lat float(10, 5),
    lon float(10, 5),
    PRIMARY KEY (crimeId)
);

-- Weather table contains weather data
CREATE TABLE s_Weather (
    weatherId int(11) NOT NULL AUTO_INCREMENT,
    date datetime NOT NULL,
    temperature_2m float(11) NOT NULL,
    humidity_2m float(11) NOT NULL,
    rain float(11) NOT NULL,
    weather_code float(11) NOT NULL,
    cloud_cover float(11) NOT NULL,
    areaName varchar(50) NOT NULL,
    PRIMARY KEY (weatherId)
);

-- Enable commit and foreign key checks to catch errors
SET FOREIGN_KEY_CHECKS=1;
COMMIT;