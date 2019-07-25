USE PySQL

SELECT TOP 1000 id, date, other, value FROM PySQL.dbo.TestSource
SELECT TOP 1000 * FROM PySQLCopy.dbo.TestDestination_Staging
SELECT TOP 1000 * FROM PySQLCopy.dbo.TestDestination



/*
INSERT INTO TestSource (id, date, other, value)
VALUES (1, GETUTCDATE(), 2, 0.5)

TRUNCATE TABLE PySQLCopy.dbo.TestDestination_Staging
TRUNCATE TABLE PySQLCopy.dbo.TestDestination
*/

