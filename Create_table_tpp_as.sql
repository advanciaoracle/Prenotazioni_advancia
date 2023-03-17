--DROP TABLE RES2.TPP;   
create table RES2.TPP as (select
        to_number(idttp)                idttp,
        to_date(dal, 'DD/MM/YYYY')      dal,
        to_date(al, 'DD/MM/YYYY')       al,
        to_number(pl)                   pl,
        nome                            nome,
        CF                              cf,
        sysdate                         dins,
        0                               fp
from res2.tpp_stage);

EXIT;