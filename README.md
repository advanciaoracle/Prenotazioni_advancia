Come caricare tutto

Caricare gli schema
	Lanciare lo script OSTELLO1.3.sql da sys
	Lanciare lo script OSTELLO2.sql da sys

Caricare le tabelle con sqlldr
	I file si devono trovare nella cartella c:\temp
	Sul prompt andare in questa cartella (cd c:\temp)
	Lanciare i seguenti comandi
		sqlldr RES2/RES2@orcl control=loadts.ctl
		sqlldr RES2/RES2@orcl control=loadtppstage.ctl
		(se vuoi i log aggiungi come argomento log=log.txt)