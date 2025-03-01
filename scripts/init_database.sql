/*
=============================================================
Create Database and Schemas
=============================================================
Script Overview:
    This script is designed to create a new database called 'DataWarehouse'. 
    It first checks if the database exists, and if so, it will drop and recreate it. 
    The script also sets up three distinct schemas within the database: 'bronze', 'silver', and 'gold'.

IMPORTANT NOTICE:
    Executing this script will remove the existing 'DataWarehouse' database if it already exists. 
    All data within the database will be erased permanently. 
    Ensure you have valid backups before proceeding with this operation.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
