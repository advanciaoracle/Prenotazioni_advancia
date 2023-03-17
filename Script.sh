#!/usr/bin/sh

# Carica tutto lo schema
sqlplus sys/sys as sysdba @OSTELLO2.sql

# Carica i dati con il sqlloader
sqlldr RES2/RES2@orcl control=loadtppstage.ctl
sqlldr RES2/RES2@orcl control=loadts.ctl

# Crea la vabella tpp (con create table as: da correggere)
sqlplus sys/sys as sysdba @Create_table_tpp_as.sql
