USE master;
GO

if exists(select 1 from sys.databases where name='Datawarehouse'
  begin
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse;
end;
go

create database DataWarehouse
go

use DataWarehouse;
go


create SCHEMA bronze;
go
create SCHEMA silver;
go
create SCHEMA gold; 
go
