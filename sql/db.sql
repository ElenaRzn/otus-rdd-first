
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;



CREATE TABLE pickup_by_hour (
hour_of_day           INT,
total_trips           INT
);

CREATE TABLE distance_statistics_by_day (
days           varchar(10),
count          double,
avg            double,
min            double,
max            double,
stddev_samp    double
);

