load data
infile 'ts.csv'
into table res2.ts
fields terminated by ";" optionally enclosed by '"'
( IDTS, COD, NOME, PD )
