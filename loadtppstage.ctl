OPTIONS (SKIP=1) 
load data
infile 'tpp.csv'
into table res2.tpp_stage
fields terminated by ";" optionally enclosed by '"'
( IDTTP, DAL, AL, PL, NOME ,CF)