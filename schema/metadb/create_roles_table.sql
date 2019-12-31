use metadb;
create table if not exists roles 
(ID          int NOT NULL, 
title        varchar(255) NOT NULL, 
description  varchar(255) NOT NULL, 
PRIMARY KEY (ID));