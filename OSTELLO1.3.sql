-- CREAZIONE SCHEMA

DROP USER RES CASCADE;
CREATE USER RES IDENTIFIED BY RES;
GRANT CONNECT,RESOURCE,DBA TO RES;

--------------------------------------------------------------------------------------------------------
-- DDL TABELLE               
--------------------------------------------------------------------------------------------------

-- TABELLA STRUTTURE

-- DROP TABLE RES.TS;
create table RES.TS(
		id_ts 	number,
        cod     varchar2(20),
		nome    varchar2(100),
		pl 		number
            );
            

-- TABELLA CLIENTI (inzialmente vuota)

--DROP TABLE RES.TC;
create table RES.TC(
		id_tc 	number,
        CF      varchar2(16),
		nome    varchar2(100)
            );

-- TABELLA PRENOTAZIONI

--DROP TABLE RES.TP;
create table RES.TP(
        id_tp   number,
		id_tc 	number,
        id_ts   number,
		dal     date,
		al 	    date,          -- si suppongano esterni COMPRESI
        n_pl    number,
        d_ins   date
            );



---------------------------------------------------------------------------------------------------------
-- CONSTRAINT TAB
----------------------------------------------------------------------------------------------------------

ALTER TABLE res.ts ADD CONSTRAINT pk_idts PRIMARY KEY (id_ts);
ALTER TABLE res.tc ADD CONSTRAINT pk_idtc PRIMARY KEY (id_tc);
ALTER TABLE res.tp ADD CONSTRAINT pk_idtp PRIMARY KEY (id_tp);
ALTER TABLE res.tp ADD CONSTRAINT fk_idtc FOREIGN KEY (id_tc) REFERENCES RES.TC(id_tc);
ALTER TABLE res.tp ADD CONSTRAINT fk_idts FOREIGN KEY (id_ts) REFERENCES RES.TS(id_ts);

---------------------------------------------------------------------------------------------------------
-- INSERT INTO TAB CON DATI FITTIZI (premessi da rispettive delete)
----------------------------------------------------------------------------------------------------------

delete RES.TS where id_ts = 1;
delete RES.TS where id_ts = 2;
delete RES.TS where id_ts = 3;

Insert into RES.TS (ID_TS,COD,NOME,PL) values ('1','A','AZALEA','10');
Insert into RES.TS (ID_TS,COD,NOME,PL) values ('2','B','BERGAMOTTO','20');
Insert into RES.TS (ID_TS,COD,NOME,PL) values ('3','C','CALLA','30');
commit;

/*
Insert into RES.TC (ID_TC,CF,NOME) values ('1','A','ALDO');
Insert into RES.TC (ID_TC,CF,NOME) values ('2','B','BEATRICE');
COMMIT;

INSERT INTO res.tp (id_tp,id_tc,id_ts,dal,al,n_pl,d_ins) VALUES (1,1,1,to_date('01/03/2023','DD/MM/YYYY'),to_date('10/03/2023','DD/MM/YYYY'),5,sysdate);
INSERT INTO res.tp (id_tp,id_tc,id_ts,dal,al,n_pl,d_ins) VALUES (3,1,1,to_date('12/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('14/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),2,sysdate);
INSERT INTO res.tp (id_tp,id_tc,id_ts,dal,al,n_pl,d_ins) VALUES (2,2,2,to_date('01/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('10/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),6,sysdate);
INSERT INTO res.tp (id_tp,id_tc,id_ts,dal,al,n_pl,d_ins) VALUES (4,2,1,to_date('12/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),to_date('14/03/2023 00:00:00','dd/mm/yyyy hh24:mi:ss'),2,sysdate);
commit;

*/

------------------------------------------------------------------------------------------------------
-- DDL SEQUENCES (servono a generare chiavi surrogate)
------------------------------------------------------------------------------------------------------

--drop sequence RES.seq_id_tc;
--drop sequence RES.seq_id_tp;

create sequence RES.seq_id_tc;
create sequence RES.seq_id_tp;


------------------------------------------------------------------------------------------------------
-- TEMPLATE PER LOG
------------------------------------------------------------------------------------------------------

create table RES.tlog(
		id_tlog 	number,
		caller 		varchar2(100),
		testo 		varchar2(100),
		dins 		date
		);

create sequence RES.seq_tlog;

create or replace package RES.pkg_utils as 
procedure plog(pcaller varchar2, ptesto varchar2);
end;
/

create or replace package body RES.pkg_utils as

  procedure plog(pcaller varchar2, ptesto varchar2) as
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
  end plog;
end;
/


--------------------------------------------------------
--  DDL for View VCAL
--------------------------------------------------------
CREATE OR REPLACE VIEW RES.VCAL(GCAL) AS 
    select  to_date('01/01/2022','DD/MM/YYYY') -1 + level as gcal
    from dual
    connect by level <=  800
;
--------------------------------------------------------
--  DDL for View VCALP
--------------------------------------------------------
CREATE OR REPLACE VIEW RES.VCALP (IDTP, IDTC, IDTS, PP, GCALP) AS 
    select a.id_tp, a.id_tc, a.id_ts,a.n_pl, b.gcal as gcalp
    from tp a, vcal b
    where trunc(gcal) between trunc(dal) and trunc(al)
    order by 1, 2, 3, 4
;
--------------------------------------------------------
--  DDL for View VTABELLONE
--------------------------------------------------------
CREATE OR REPLACE VIEW RES.VTABELLONE ("IDTS", "GCAL", "TOTPP") AS 
    select b.idts, a.gcal, sum(pp) totpp
    from  vcal a, vcalp b
    where a.gcal = b.gcalp (+) 
    group by  b.idts, a.gcal
    order by a.gcal
;


------------------------------------------------------------------------------------------------------------------
--- MAIN PROCEDURE PER LE PRENOTAZIONI CHE RICHIAMA LE PRECEDENTI
-------------------------------------------------------------------------------------------------------------------


create PROCEDURE     RES.P_MAIN(p_dal IN DATE, p_al IN DATE, p_pl IN NUMBER, p_nome IN VARCHAR2, p_cf IN VARCHAR2)
IS

v_idtc number;
v_idts number           := 0;

cursor c1 is 
    WITH cte1 AS(
        SELECT gcal, id_ts, pl
        FROM vcal, ts
        WHERE gcal between p_dal and p_al
        ORDER BY nome, gcal)
    SELECT cte1.gcal, cte1.id_ts, cte1.pl - NVL(vtabellone.totpp, 0) AS plib
    FROM cte1 LEFT OUTER JOIN vtabellone
    ON (cte1.gcal = vtabellone.gcal AND cte1.id_ts = vtabellone.idts);

rec1        c1%rowtype;
v_lun       number;             -- registra lunghezza soggiorno
v_loc       number;             -- registra struttura
v_c         number      := 0;   -- counter giorni liberi

BEGIN

RES.pkg_utils.plog('P_MAIN','INIZIO PROCEDURA');

-- vediamo se abbiamo disponibilità

select p_al - p_dal  + 1 into v_lun from dual;

RES.pkg_utils.plog('check_disp','SERVONO '||p_pl||' posti per giorni '|| v_lun);

open c1;
loop
    fetch c1 into rec1;
    exit when c1%notfound;
    RES.pkg_utils.plog('CHECK_DISP','REC1 IS '|| rec1.plib || ' ' || rec1.id_ts);

    if v_loc <> rec1.id_ts then
        v_c := 0;                   -- ho cambiato struttura, riazzero il counter
    end if;

    RES.pkg_utils.plog('CHECK_DISP','CERCO '|| p_pl || ' POSTI E NE HO '|| rec1.plib || ' NELLA STRUTT ' ||  rec1.id_ts);

    IF rec1.plib >= p_pl THEN       -- se ho posti liberi avanzo il counter, altrimenti no(n faccio nulla)
        v_c := v_c + 1;
    END IF;

    if v_c = v_lun then             -- se il counter ha trovato tutti i giorni disponibili torno la struttura, altrimenti passerò a quella successiva
        v_idts := v_loc;
        exit;
    end if;
    
    RES.pkg_utils.plog('CHECK_DISP',v_c);
    
    v_loc := rec1.id_ts;            --tengo in memoria la struttura su cui ero

end loop;
close c1;

-- ora vediamo se il cliente non è inserito lo inseriamo

select count(id_tc) 
into v_c                            -- anche se l'ho usato prima, non mi riservirà più quindi posso riciclare la var
from RES.tc
where cf = p_cf;

IF v_c = 0 and v_idts <> 0 then  
    INSERT INTO RES.TC(ID_TC, CF, NOME) VALUES (RES.seq_id_tc.nextval, p_cf, p_nome);               -- c'è la possibilità di returning sulla insert
    RES.pkg_utils.plog('REG UTENTE ','nuovo utente ');
END IF;

-- avendo registrato il cliente abbiamo un suo id e possiamo fare l'insert nella prenotazioni

IF  v_idts <> 0 then     
    select id_tc into v_idtc from RES.tc where cf = p_cf;
    INSERT INTO RES.TP(id_tp, ID_TC, ID_TS, DAL, AL, N_PL, D_INS) VALUES (RES.seq_id_tp.nextval, v_idtc, v_idts, p_dal, p_al, p_pl, sysdate);
    RES.pkg_utils.plog('REG PRENOTAZIONE','nuova prenotazione ');
ELSE RES.pkg_utils.plog('P_MAIN','strutture al completo nel periodo '|| p_dal ||' - '|| p_al);
END IF;

RES.pkg_utils.plog('P_MAIN','FINE PROCEDURA');

COMMIT;

exception
        when others then
         rollback;
         RES.pkg_utils.plog('registrazione prenotazione', 'SQLCODE: '||sqlcode|| ' SQLERRM: '|| substr(sqlerrm, 1, 50));

END;

/



-- inseriamo qualche prenotaazione di prova

-- truncate table RES.tlog;
-- truncate table RES.tp;
-- truncate table RES.tc;





/*

BEGIN
RES.P_MAIN('17/12/2022', '18/12/2022', 4, 'ANNA', 'ASIJBDKJA');
RES.P_MAIN('12/12/2022', '21/12/2022', 5, 'MARCO', 'MASOIHDFW1');
RES.P_MAIN('18/12/2022', '25/12/2022', 3, 'LUCA', 'LSDIIWXJ2P');
RES.P_MAIN('15/12/2022', '19/12/2022', 5, 'UGO', 'UGOWEINFO');
RES.P_MAIN('16/12/2022', '19/12/2022', 10, 'MARCO', 'MASOIHDFW1');
RES.P_MAIN('20/12/2022', '22/12/2022', 20, 'UGO', 'UGOWEINFO');
RES.P_MAIN('18/12/2022', '22/12/2022', 30, 'LEONARDO', 'LASODINHOI');
END;
/

*/

/*
BEGIN
RES.P_MAIN('20/12/2023', '18/12/2023', -2, 'ANNA', 'ASIJBDKJA');        -- da strutture al completo, non è gestito bene ma almeno non si rompe
END;
*/