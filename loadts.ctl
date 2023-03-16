load data
infile 'c:\temp\ts.csv'
into table res2.ts
fields terminated by ";" optionally enclosed by '"'
( IDTS, COD, NOME, PD )
