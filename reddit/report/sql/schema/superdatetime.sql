-- To obtain posting schedules we query a table for a count of posts within a
-- time range, grouped by year, month, week, day, hour. Unfortunately, if a
-- period of time doesn't have any posts then it's not represented in the
-- return and schedules aren't represented exactly right.
--
-- The solution is to left join to another table you know will contain a value.
-- Even if the corresponding count will be 0, we'll at least receive a 0 for
-- the time period.
--
-- There's a module you have to load to get generate_series() to work. On Debian
-- it was as simple as "apt-get postgres-somethingorother"... just google the
-- error you get and figure it out then drop in here and document it please!
CREATE TABLE superdatetime (base timestamp PRIMARY KEY);
INSERT INTO superdatetime (
    SELECT generate_series(timezone('UTC', '2010-01-01 00:00:00'),
            timezone('UTC','2030-12-31 23:59:59'), '1 hour'::interval));
