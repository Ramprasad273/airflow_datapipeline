CREATE TABLE airflow__extra_conf(
  conf_name  VARCHAR (255) PRIMARY KEY,
  conf_value VARCHAR (255) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_table(month varchar (20),pick_up varchar (50),drop_off varchar (50),passenger_count int);
CREATE TABLE IF NOT EXISTS popular_destination_history(month varchar (20),pick_up varchar (50),drop_off varchar (50),rank int);
CREATE TABLE IF NOT EXISTS popular_destination_current_month(pick_up varchar (50),drop_off varchar (50),rank int);
CREATE TABLE IF NOT EXISTS location_lookup(location_id int,borough varchar(15),zone varchar(45),service_zone varchar(15));
TRUNCATE TABLE location_lookup;
