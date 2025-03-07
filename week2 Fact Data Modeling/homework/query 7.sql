--A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced (
     month DATE,                     -- The month for aggregation
     host TEXT,                      -- The host identifier
     hit_array INT,                  -- Count of total hits
     unique_visitors TEXT[],      -- Count of unique users
    PRIMARY KEY (month, host)       -- Ensure uniqueness of each host per month
);


