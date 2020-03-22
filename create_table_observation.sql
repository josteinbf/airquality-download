-- DROP TABLE IF EXISTS observation;

CREATE TABLE observation (
    station_id BIGINT NOT NULL REFERENCES station(station_id),
    quantity_id BIGINT NOT NULL REFERENCES quantity(quantity_id),
    datetime_begin TIMESTAMP WITH TIME ZONE NOT NULL,
    datetime_end TIMESTAMP WITH TIME ZONE,
    concentration FLOAT,
    unit_of_measurement TEXT,
    validity INT,
    verification INT,
    PRIMARY KEY(station_id, quantity_id, datetime_begin)
);
COMMIT;

SELECT create_hypertable(
    'observation', 'datetime_begin',
    'station_id', 5000,
    chunk_time_interval => interval '3 months'
);
