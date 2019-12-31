use metadb;
create table if not exists users 
(ID        varchar(255) NOT NULL, 
first_name varchar(255) NOT NULL, 
last_name  varchar(255) NOT NULL, 
email      varchar(255) NOT NULL, 
role_id    int,
FOREIGN KEY (role_id) REFERENCES roles(ID)  , 
PRIMARY KEY (ID)
);