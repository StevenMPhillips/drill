use dfs.home;
create view beacon_aster as select columns[2] as osid, columns[3] as browsertype,
columns[4] as ip_address, columns[7] as c1, columns[13] as c7,
columns[24] as uid_hash, columns[25] as uid, cast(columns[26] as timestamp) as recorded,
columns[28] as uahash, cast(columns[29] as bigint) as pattern_id, columns[31] as country from `/Users/sphillips/cust-d1.tsv`;

select * from beacon_aster;