-- CREAZIONE SCHEMA

DROP USER RES2 CASCADE;
CREATE USER RES2 IDENTIFIED BY RES2;
GRANT CONNECT,RESOURCE,DBA TO RES2;

--------------------------------------------------------------------------------------------------------
-- DDL TABELLE               
--------------------------------------------------------------------------------------------------

-- TABELLA STRUTTURE

-- DROP TABLE RES2.TS;
create table RES2.TS(
		idts 	number,
        cod     varchar2(20),
		nome    varchar2(100),
		pd 		number
            );
            

-- TABELLA CLIENTI (inzialmente vuota)

--DROP TABLE RES2.TC;
create table RES2.TC(
		idtc 	number,
        CF      varchar2(16),
		nome    varchar2(100)
            );

-- TABELLA PRENOTAZIONI

--DROP TABLE RES2.TP;
create table RES2.TP(
        idtp    number,
		idtc    number,
        idts    number,
		dal     date,
		al 	    date,          -- si suppongano esterni COMPRESI
        pl      number,
        dins    date
            );

-- TABELLA POTENZIALI PRENOTAZIONI (facciamo una tab intermedia per non far arrabbiare sql loader con le date, poi facciamo il to_date)

--DROP TABLE RES2.TPP_STAGE;
create table RES2.TPP_STAGE(
        idttp VARCHAR2(30),
        dal   VARCHAR2(30),
        al    VARCHAR2(30),
        pl    VARCHAR2(30),
        nome  VARCHAR2(30),
        CF    VARCHAR2(16),
        dins  VARCHAR2(30),
        fp    VARCHAR2(30)
            );

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


---------------------------------------------------------------------------------------------------------
-- CONSTRAINT TAB
----------------------------------------------------------------------------------------------------------

ALTER TABLE RES2.ts ADD CONSTRAINT pk_idts PRIMARY KEY (idts);
ALTER TABLE RES2.tc ADD CONSTRAINT pk_idtc PRIMARY KEY (idtc);
ALTER TABLE RES2.tp ADD CONSTRAINT pk_idtp PRIMARY KEY (idtp);
ALTER TABLE RES2.tp ADD CONSTRAINT fk_idtc FOREIGN KEY (idtc) REFERENCES RES2.TC(idtc);
ALTER TABLE RES2.tp ADD CONSTRAINT fk_idts FOREIGN KEY (idts) REFERENCES RES2.TS(idts);


/*
---------------------------------------------------------------------------------------------------------
-- INSERT INTO TAB CON DATI FITTIZI (premessi da rispettive delete)
----------------------------------------------------------------------------------------------------------

delete RES2.TS where idts = 1;
delete RES2.TS where idts = 2;
delete RES2.TS where idts = 3;

Insert into RES2.TS (idts,COD,NOME,pd) values ('1','A','AZALEA','10');                      -- ORA FACCIAMO L'INSERT STRUTTURA CON L'SQL LOADER
Insert into RES2.TS (idts,COD,NOME,pd) values ('2','B','BERGAMOTTO','20');
Insert into RES2.TS (idts,COD,NOME,pd) values ('3','C','CALLA','30');
commit;


Insert into RES2.TC (idtc,CF,NOME) values ('1','A','ALDO');
Insert into RES2.TC (idtc,CF,NOME) values ('2','B','BEATRICE');
COMMIT;

INSERT INTO RES2.tp (idtp,idtc,idts,dal,al,pl,dins) VALUES (1,1,1,to_date('01/03/2023','DD/MM/YYYY'),to_date('10/03/2023','DD/MM/YYYY'),5,sysdate);
INSERT INTO RES2.tp (idtp,idtc,idts,dal,al,pl,dins) VALUES (3,1,1,to_date('12/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('14/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),2,sysdate);
INSERT INTO RES2.tp (idtp,idtc,idts,dal,al,pl,dins) VALUES (2,2,2,to_date('01/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('10/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),6,sysdate);
INSERT INTO RES2.tp (idtp,idtc,idts,dal,al,pl,dins) VALUES (4,2,1,to_date('12/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('14/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),2,sysdate);
commit;

*/

------------------------------------------------------------------------------------------------------
-- DDL SEQUENCES (servono a generare chiavi surrogate)
------------------------------------------------------------------------------------------------------

--drop sequence RES2.seq_idtc;
--drop sequence RES2.seq_idtp;

create sequence RES2.seq_idtc;
create sequence RES2.seq_idtp;


------------------------------------------------------------------------------------------------------
-- TEMpdATE PER LOG
------------------------------------------------------------------------------------------------------

create table RES2.tlog(
		id_tlog 	number,
		caller 		varchar2(100),
		testo 		varchar2(100),
		dins 		date
		);

create sequence RES2.seq_tlog;

create or replace package RES2.pkg_utils as 
procedure pdog(pcaller varchar2, ptesto varchar2);
end;
/

create or replace package body RES2.pkg_utils as

  procedure pdog(pcaller varchar2, ptesto varchar2) as
  pragma autonomous_transaction;
  begin
    insert into tlog (
            id_tlog,                        -- ID Messaggio di Log mediante sequence SEQTLOG
            caller,                         -- Identificativo procedura che invia messaggio di log
            testo,                          -- Testo messaggio
            dins                            -- Data e ora log (popolato mediante sysdate)
                          )
        values (
            seq_tlog.nextval,
            upper(substr(pcaller,1,100)),
            upper(substr(ptesto,1,100)),
            sysdate
                );
    commit;  --> valido solo all'interno di questa transazione
    exception
        when others then
            dbms_output.put_line('SQLCODE: '||sqlcode|| ' SQLERRM: '|| substr(sqlerrm, 1, 100));
            rollback; --> valido solo all'interno di questa transazione
  end pdog;
end;
/


--------------------------------------------------------
--  DDL for View VCAL
--------------------------------------------------------
CREATE OR REPLACE VIEW RES2.VCAL(GCAL) AS 
    select  to_date('01/01/2022','DD/MM/YYYY') -1 + level as gcal
    from dual
    connect by level <=  800
;
--------------------------------------------------------
--  DDL for View VCALP
--------------------------------------------------------
CREATE OR REPLACE VIEW RES2.VCALP (IDTP, IDTC, IDTS, PP, GCALP) AS 
    select a.idtp, a.idtc, a.idts,a.pl, b.gcal as gcalp
    from tp a, vcal b
    where trunc(gcal) between trunc(dal) and trunc(al)
    order by 1, 2, 3, 4
;
--------------------------------------------------------
--  DDL for View VTABELLONE
--------------------------------------------------------
CREATE OR REPLACE VIEW RES2.VTABELLONE ("IDTS", "GCAL", "TOTPP") AS 
    select b.idts, a.gcal, sum(pp) totpp
    from  vcal a, vcalp b
    where a.gcal = b.gcalp (+) 
    group by  b.idts, a.gcal
    order by a.gcal
;


------------------------------------------------------------------------------------------------------------------
--- MAIN PROCEDURE PER LE PRENOTAZIONI CHE RICHIAMA LE PRECEDENTI
-------------------------------------------------------------------------------------------------------------------

create or replace PROCEDURE RES2.P1(p_dal IN DATE, p_al IN DATE, p_pd IN NUMBER, p_nome IN VARCHAR2, p_cf IN VARCHAR2)
IS

v_idtc number;
v_idts number           := 0;

cursor c1 is 
    WITH cte1 AS(
        SELECT gcal, idts, pd
        FROM vcal, ts
        WHERE gcal between p_dal and p_al
        ORDER BY nome, gcal)
    SELECT cte1.gcal, cte1.idts, cte1.pd - NVL(vtabellone.totpp, 0) AS pdib
    FROM cte1 LEFT OUTER JOIN vtabellone
    ON (cte1.gcal = vtabellone.gcal AND cte1.idts = vtabellone.idts);

rec1        c1%rowtype;
v_lun       number      := p_al - p_dal  + 1;       -- registra lunghezza soggiorno
v_loc       number;                                 -- registra struttura
v_c         number      := 0;                       -- counter giorni liberi

BEGIN

RES2.pkg_utils.pdog('P1','INIZIO PROCEDURA');

-- vediamo se abbiamo disponibilità

RES2.pkg_utils.pdog('check_disp','SERVONO '||p_pd||' posti per giorni '|| v_lun);

open c1;
loop
    fetch c1 into rec1;
    exit when c1%notfound;
    RES2.pkg_utils.pdog('CHECK_DISP','REC1 IS '|| rec1.pdib || ' ' || rec1.idts);

    if v_loc <> rec1.idts then
        v_c := 0;                   -- ho cambiato struttura, riazzero il counter
    end if;

    RES2.pkg_utils.pdog('CHECK_DISP','CERCO '|| p_pd || ' POSTI E NE HO '|| rec1.pdib || ' NELLA STRUTT ' ||  rec1.idts);

    if rec1.pdib >= p_pd then       -- se ho posti liberi avanzo il counter, altrimenti no(n faccio nulla)
        v_c := v_c + 1;
    end if;    

    if v_c = v_lun then             -- se il counter ha trovato tutti i giorni disponibili torno la struttura, altrimenti passerò a quella successiva
        v_idts := v_loc;
        exit;
    end if;
    
    RES2.pkg_utils.pdog('CHECK_DISP',v_c);
    
    v_loc := rec1.idts;            --tengo in memoria la struttura su cui ero

end loop;
close c1;

-- ora vediamo se il cliente non è inserito lo inseriamo

select count(idtc) 
into v_c                            -- anche se l'ho usato prima, non mi riservirà più quindi posso riciclare la var
from RES2.tc
where cf = p_cf;

IF v_c = 0 and v_idts <> 0 then  
    INSERT INTO RES2.TC(idtc, CF, NOME) VALUES (RES2.seq_idtc.nextval, p_cf, p_nome);               -- c'è la possibilità di returning sulla insert
    RES2.pkg_utils.pdog('REG UTENTE ','nuovo utente ');
END IF;

-- avendo registrato il cliente abbiamo un suo id e possiamo fare l'insert nella prenotazioni

IF  v_idts <> 0 then     
    select idtc into v_idtc from RES2.tc where cf = p_cf;
    INSERT INTO RES2.TP(idtp, idtc, idts, DAL, AL, pl, dins) VALUES (RES2.seq_idtp.nextval, v_idtc, v_idts, p_dal, p_al, p_pd, sysdate);
    RES2.pkg_utils.pdog('REG PRENOTAZIONE','nuova prenotazione ');
ELSE RES2.pkg_utils.pdog('P1','strutture al compdeto nel periodo '|| p_dal ||' - '|| p_al);
END IF;

RES2.pkg_utils.pdog('P1','FINE PROCEDURA');

COMMIT;

exception
        when others then
         rollback;
         RES2.pkg_utils.pdog('registrazione prenotazione', 'SQLCODE: '||sqlcode|| ' SQLERRM: '|| substr(sqlerrm, 1, 50));

END;

/



-- inseriamo qualche prenotaazione di prova

-- truncate table RES2.tlog;
-- truncate table RES2.tp;
-- truncate table RES2.tc;


/*

BEGIN
RES2.P1('17/12/2022', '18/12/2022', 4, 'ANNA', 'ASIJBDKJA');
RES2.P1('12/12/2022', '21/12/2022', 5, 'MARCO', 'MASOIHDFW1');
RES2.P1('18/12/2022', '25/12/2022', 3, 'LUCA', 'LSDIIWXJ2P');
RES2.P1('15/12/2022', '19/12/2022', 5, 'UGO', 'UGOWEINFO');
RES2.P1('16/12/2022', '19/12/2022', 10, 'MARCO', 'MASOIHDFW1');
RES2.P1('20/12/2022', '22/12/2022', 20, 'UGO', 'UGOWEINFO');
RES2.P1('18/12/2022', '22/12/2022', 30, 'LEONARDO', 'LASODINHOI');
RES2.P1('20/12/2023', '18/12/2023', -2, 'ANNA', 'ASIJBDKJA');        -- da strutture al compdeto, non è gestito bene ma almeno non si rompe
END;
/

*/
