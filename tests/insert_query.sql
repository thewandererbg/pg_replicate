
insert into all_postgres_types (col_smallint, col_integer, col_bigint, col_decimal, col_numeric, col_real
                               , col_double_precision, col_money, col_char, col_varchar, col_text, col_bytea
                               , col_boolean, col_date, col_time, col_timetz, col_timestamp, col_timestamptz
                               , col_interval, col_json, col_jsonb, col_xml, col_uuid, col_inet, col_cidr, col_macaddr
                               , col_macaddr8, col_point, col_line, col_lseg, col_box, col_path, col_polygon, col_circle
                               , col_bit, col_bit_varying, col_tsquery, col_tsvector, col_enum, col_array_int
                               , col_array_text, col_array_timestamp, col_array_timestamptz, col_array_date, col_array_time, col_array_timetz
                               , col_range_int4, col_range_num, col_range_ts, col_range_tstz, col_range_date)

select floor(random() * 100)::smallint as col_smallint
     , floor(random() * 1000)::integer as col_integer
     , floor(random() * 100000)::bigint as col_bigint
     , round((random() * 1000)::numeric, 2) as col_decimal
     , round((random() * 1000)::numeric, 2) as col_numeric
     , random()::real as col_real
     , random()::double precision as col_double_precision
     , (random() * 1000)::numeric::money as col_money
     , 'abc       ' as col_char
     , 'random text' as col_varchar
     , 'some long text content' as col_text
     , decode('DEADBEEF', 'hex') as col_bytea
     , (random() > 0.5) as col_boolean
     , now()::date as col_date
     , now()::time as col_time
     , now()::timetz as col_timetz
     , now() as col_timestamp
     , now()::timestamptz as col_timestamptz
     , '1 hour'::interval as col_interval
     , '{"a":1, "b":2}' as col_json
     , '{"a": 3, "b": 4}'::jsonb as col_jsonb
     , '<root><val>1</val></root>'::xml as col_xml
     , gen_random_uuid() as col_uuid
     , '192.168.1.1' as col_inet
     , '192.168.0.0/16' as col_cidr
     , '08:00:2b:01:02:03' as col_macaddr
     , '08:00:2b:01:02:03:04:05' as col_macaddr8
     , point(1, 2) as col_point
     , '[(0,0),(1,1)]'::line as col_line
     , '[(1,1),(2,2)]'::lseg as col_lseg
     , '((0,0),(1,1))'::box as col_box
     , '((0,0),(1,1),(2,2))'::path as col_path
     , '((0,0),(1,1),(2,2),(0,0))'::polygon as col_polygon
     , '<(0,0),1>'::circle as col_circle
     , B'10101010' as col_bit
     , B'1010101010101010' as col_bit_varying
     , to_tsquery('english', 'test') as col_tsquery
     , to_tsvector('english', 'this is a test') as col_tsvector
     , 'happy'::mood_enum as col_enum
     , array [1,2,3] as col_array_int
     , array ['a','b','c'] as col_array_text
     , array(select now() + (i * interval '1 hour') from generate_series(0, 4) as i) as col_array_timestamp
     , array(select now() + (i * interval '1 hour') from generate_series(0, 4) as i) as col_array_timestamptz
     , array(select current_date + i from generate_series(0, 4) as i) as col_array_date
     , array(select (current_time + (i * interval '1 hour')) from generate_series(0, 4) as i) as col_array_time
     , array(select (current_time + (i * interval '1 hour')) from generate_series(0, 4) as i) as col_array_timetz
     , int4range(1, 10) as col_range_int4
     , numrange(1.1, 9.9) as col_range_num
     , tsrange(now()::timestamp, (now() + interval '1 hour')::timestamp) as col_range_ts
     , tstzrange(now(), now() + interval '1 hour') as col_range_tstz
     , daterange(current_date, current_date + 7) as col_range_date;
