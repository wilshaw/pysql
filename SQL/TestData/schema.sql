
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'PySQL')
CREATE DATABASE PySQL

USE PySQL

IF OBJECT_ID('PySQL.dbo.TestSource') IS NOT NULL DROP TABLE dbo.TestSource
CREATE TABLE dbo.TestSource
(
	id INT NOT NULL,
	date DATETIME NOT NULL,
	other INT NOT NULL,
	value REAL NULL,
	PRIMARY KEY (id, date, other)
)



IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'PySQLCopy')
CREATE DATABASE PySQLCopy

USE PySQLCopy
IF OBJECT_ID('PySQLCopy.dbo.TestDestination') IS NOT NULL DROP TABLE dbo.TestDestination
CREATE TABLE dbo.TestDestination
(
	id INT NOT NULL,
	date DATETIME NOT NULL,
	other INT NOT NULL,
	value REAL NULL,
	PRIMARY KEY (id, date, other)
)



