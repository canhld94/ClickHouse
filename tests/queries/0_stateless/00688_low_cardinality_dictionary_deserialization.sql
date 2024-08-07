drop table if exists lc_dict_reading;
create table lc_dict_reading (val UInt64, str LowCardinality(String), pat String) engine = MergeTree order by val SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into lc_dict_reading select number, if(number < 8192 * 4, number % 100, number) as s, s from system.numbers limit 1000000;
select sum(toUInt64(str)), sum(toUInt64(pat)) from lc_dict_reading where val < 8129 or val > 8192 * 4;
drop table if exists lc_dict_reading;
