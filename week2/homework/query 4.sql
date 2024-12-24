--A datelist_int generation query.
-- 1st - create the new column in table 'user_devices_cumulated'
ALTER TABLE user_devices_cumulated
ADD COLUMN datelist_int BIGINT[];

-- 2nd - populate column with the integer representation of the device_activity_datelist
UPDATE user_devices_cumulated
SET datelist_int = ARRAY(
    SELECT EXTRACT(EPOCH FROM unnest(device_activity_datelist))::BIGINT
);