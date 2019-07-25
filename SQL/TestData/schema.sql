use PySQL

IF OBJECT_ID('PySQL.dbo.TestSource') IS NOT NULL DROP TABLE dbo.TestSource
CREATE TABLE dbo.TestSource
(
	id INT NOT NULL,
	date DATETIME NOT NULL,
	other INT NOT NULL,
	value REAL NULL,
	PRIMARY KEY (id, date, other)
)

IF OBJECT_ID('PySQL.dbo.TestDestination') IS NOT NULL DROP TABLE dbo.TestDestination
CREATE TABLE dbo.TestDestination
(
	id INT NOT NULL,
	date DATETIME NOT NULL,
	other INT NOT NULL,
	value REAL NULL,
	PRIMARY KEY (id, date, other)
)

IF OBJECT_ID('PySQL.dbo.TestDestination_Staging') IS NOT NULL DROP TABLE dbo.TestDestination_Staging
CREATE TABLE dbo.TestDestination_Staging
(
	id INT NOT NULL,
	date DATETIME NOT NULL,
	other INT NOT NULL,
	value REAL NULL,
	PRIMARY KEY (id, date, other)
)


