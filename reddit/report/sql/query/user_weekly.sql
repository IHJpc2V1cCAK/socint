SELECT *
FROM crosstab('
    SELECT CASE EXTRACT(DOW FROM superdatetime.base)
                WHEN 0 THEN ''0Sun''
                WHEN 1 THEN ''1Mon''
                WHEN 2 THEN ''2Tues''
                WHEN 3 THEN ''3Wed''
                WHEN 4 THEN ''4Thu''
                WHEN 5 THEN ''5Fru''
                WHEN 6 THEN ''6Sat''
            END as dow,
        ''h''||to_char(EXTRACT(HOUR FROM superdatetime.base), ''FM00'') as hour,
        count(author)
    FROM superdatetime
        LEFT JOIN {table} td ON
            to_char(EXTRACT(YEAR FROM td.created_utc), ''FM0000'')||to_char(EXTRACT(MONTH FROM td.created_utc), ''FM00'')||to_char(EXTRACT(DAY FROM td.created_utc), ''FM00'')||to_char(EXTRACT(HOUR FROM td.created_utc), ''FM00'') =
                to_char(EXTRACT(YEAR FROM superdatetime.base), ''FM0000'')||to_char(EXTRACT(MONTH FROM superdatetime.base), ''FM00'')||to_char(EXTRACT(DAY FROM superdatetime.base), ''FM00'')||to_char(EXTRACT(HOUR FROM superdatetime.base), ''FM00'')
        and lower(td.author) = ''{user}''
    GROUP BY CASE EXTRACT(DOW FROM superdatetime.base)
                WHEN 0 THEN ''0Sun''
                WHEN 1 THEN ''1Mon''
                WHEN 2 THEN ''2Tues''
                WHEN 3 THEN ''3Wed''
                WHEN 4 THEN ''4Thu''
                WHEN 5 THEN ''5Fru''
                WHEN 6 THEN ''6Sat''
            END, ''h''||to_char(EXTRACT(HOUR FROM superdatetime.base), ''FM00'')
    ORDER BY dow, hour
') AS final_result(dow TEXT,
                h1 BIGINT,
                h2 BIGINT,
                h3 BIGINT,
                h4 BIGINT,
                h5 BIGINT,
                h6 BIGINT,
                h7 BIGINT,
                h8 BIGINT,
                h9 BIGINT,
                h10 BIGINT,
                h11 BIGINT,
                h12 BIGINT,
                h13 BIGINT,
                h14 BIGINT,
                h15 BIGINT,
                h16 BIGINT,
                h17 BIGINT,
                h18 BIGINT,
                h19 BIGINT,
                h20 BIGINT,
                h21 BIGINT,
                h22 BIGINT,
                h23 BIGINT,
                h24 BIGINT);
