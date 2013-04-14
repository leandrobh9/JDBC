
create user leandro ;

CREATE TABLESPACE leandro DATAFILE 'leandro' 10M;

-- selecionar os tablespaces do banco
select distinct tablespace_name from user_tables;

-- tablespace padrao (se for criado uma tabela, sera criado nesse tablespace)
-- Se um usuário criar um objeto, tal como uma tabela ou um índice, sem especificar o tablespace, o Oracle o cria no tablespace padrão do usuário
select default_tablespace from user_users;

-- O tablespace SYSTEM (tablespace de sistema) é uma parte obrigatória de todo banco de dados Oracle. É onde o Oracle armazena todas as informações necessárias para o seu próprio gerenciamento.
-- SYSTEM é o tablespace mais crítico do banco de dados porque ele contém o dicionário de dados. Se por algum motivo ele se tornar indisponível, a instância do Oracle abortará. Por esse motivo, o tablespace SYSTEM nunca pode ser colocado offline, ao contrário de um tablespace comum como, por exemplo, o tablespace USERS.

-- criacao de um tablespace
CREATE TABLESPACE leandro LOGGING DATAFILE 'D:\TI\BD\Oracle\meu_tablespace.dbf' SIZE 10m AUTOEXTEND ON NEXT 100m EXTENT MANAGEMENT LOCAL;

-- apagar tablespace
drop tablespace leandro;

-- criacao do usuario e sua vinculacao com o tablespace
CREATE USER userleandro IDENTIFIED BY Leandro123 DEFAULT TABLESPACE leandro QUOTA UNLIMITED ON  leandro;

-- autoriza o usuario a logar
GRANT CREATE SESSION TO userleandro;

GRANT CREATE TABLE TO userleandro;


