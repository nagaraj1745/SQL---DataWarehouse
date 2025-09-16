
-- ===================================================
-- 1. Create Database & Schemas
-- ===================================================

CREATE DATABASE HR_Project;
GO

USE HR_Project;
GO

CREATE SCHEMA Staging;
GO

CREATE SCHEMA DW;
GO


-- ===================================================
-- 2. Staging Tables (Raw Data)
-- ===================================================

-- Employee Staging
IF OBJECT_ID('Staging.Employee_raw','U') IS NOT NULL
    DROP TABLE Staging.Employee_raw;
GO

CREATE TABLE Staging.Employee_raw (
    EmployeeID   VARCHAR(50),         
    FirstName    VARCHAR(100),
    LastName     VARCHAR(100),
    Gender       VARCHAR(50),
    Department   VARCHAR(100),
    JobTitle     VARCHAR(100),
    HireDate     VARCHAR(50),          
    Salary       VARCHAR(50),             
    ManagerID    VARCHAR(50),
    Email        VARCHAR(150),
    Location     VARCHAR(100)
);
GO

-- Performance Staging
IF OBJECT_ID('Staging.Performance_raw','U') IS NOT NULL
    DROP TABLE Staging.Performance_raw;
GO

CREATE TABLE Staging.Performance_raw (
    ReviewID          VARCHAR(50),
    EmployeeID        VARCHAR(50),         
    ReviewDate        VARCHAR(50),
    PerformanceScore  VARCHAR(50),   
    Reviewer          VARCHAR(100),
    Comments          VARCHAR(MAX)
);
GO


-- ===================================================
-- 3. DW Tables (Dimensions & Fact)
-- ===================================================

CREATE TABLE DW.DimEmployee (
    EmpID        INT PRIMARY KEY,        
    EmployeeName NVARCHAR(150) NOT NULL, 
    Gender       NVARCHAR(20)  NULL,     
    DOB          DATE          NULL,     
    HireDate     DATE          NULL,     
    JobTitle     NVARCHAR(100) NULL,     
    Location     NVARCHAR(100) NULL,     
    Department   NVARCHAR(100) NULL,     
    ExpLevel     NVARCHAR(50)  NULL      
);
GO

CREATE TABLE DW.FactPerformance (
    EmpID         INT NOT NULL,  
    ReviewDate    DATE NOT NULL,  
    Rating        INT NULL,  
    Bonus         DECIMAL(10,2) NULL,  
    AnnualReview  NVARCHAR(20) NULL,  
    Promoted      NVARCHAR(3) NULL
);
GO

-- DimDate
IF OBJECT_ID('DW.DimDate','U') IS NOT NULL
    DROP TABLE DW.DimDate;
GO

CREATE TABLE DW.DimDate (
    DateKey     INT PRIMARY KEY,        
    FullDate    DATE NOT NULL,
    Day         INT,
    Month       INT,
    MonthName   NVARCHAR(20),
    Quarter     INT,
    Year        INT,
    WeekOfYear  INT,
    DayOfWeek   INT,
    DayName     NVARCHAR(20)
);
GO

-- DimTitle
CREATE TABLE DW.DimTitle (
    TitleID     INT IDENTITY(1,1) PRIMARY KEY,
    JobTitle    NVARCHAR(100) NOT NULL,
    Description NVARCHAR(200) NULL
);
GO


-- ===================================================
-- 4. Bulk Insert CSVs
-- ===================================================

BULK INSERT Staging.Employee_raw
FROM 'C:\Users\nraj6\source\Datasets\Employee.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

BULK INSERT Staging.Performance_raw
FROM 'C:\Users\nraj6\source\Datasets\Performance.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO


-- ===================================================
-- 5. Stored Procedures (ETL: Clean & Load)
-- ===================================================

-- SP: Load DimEmployee
IF OBJECT_ID('DW.sp_LoadDimEmployee','P') IS NOT NULL
    DROP PROCEDURE DW.sp_LoadDimEmployee;
GO

CREATE PROCEDURE DW.sp_LoadDimEmployee
AS
BEGIN
    SET NOCOUNT ON;

    WITH CTE_Employee AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY TRIM(EmployeeID) ORDER BY HireDate DESC) AS rn
        FROM Staging.Employee_raw
    )
    INSERT INTO DW.DimEmployee
    (EmpID, EmployeeName, Gender, DOB, HireDate, JobTitle, Location, Department, ExpLevel)
    SELECT 
        TRY_CAST(TRIM(EmployeeID) AS INT) AS EmpID,
        UPPER(LEFT(FirstName,1)) + LOWER(SUBSTRING(FirstName,2,LEN(FirstName))) + ' ' +
        UPPER(LEFT(LastName,1)) + LOWER(SUBSTRING(LastName,2,LEN(LastName))) AS EmployeeName,
        CASE 
            WHEN Gender = 'M' THEN 'Male'
            WHEN Gender = 'F' THEN 'Female'
            WHEN Gender IS NULL OR Gender = '?' THEN 'Not declared'
            ELSE UPPER(LEFT(Gender,1)) + LOWER(SUBSTRING(Gender,2,LEN(Gender)))
        END AS Gender,
        TRY_CONVERT(DATE, DOB, 104),
        TRY_CONVERT(DATE, HireDate, 104),
        JobTitle,
        CASE 
            WHEN LOWER(Location) LIKE '%bangalore%' THEN 'Bangalore'
            WHEN LOWER(Location) LIKE '%dubai%' THEN 'Dubai'
            WHEN LOWER(Location) LIKE '%hyderabad%' THEN 'Hyderabad'
            WHEN LOWER(Location) LIKE '%london%' THEN 'London'
            WHEN LOWER(Location) LIKE '%new york%' OR LOWER(Location) LIKE '%nyc%' THEN 'New York'
            WHEN LOWER(Location) LIKE '%singapore%' THEN 'Singapore'
            ELSE 'Unknown'
        END AS Location,
        CASE 
            WHEN LOWER(Department) LIKE '%fin%' THEN 'Finance'
            WHEN LOWER(Department) LIKE '%it%' THEN 'IT'
            WHEN LOWER(Department) LIKE '%hr%' THEN 'HR'
            WHEN LOWER(Department) LIKE '%analytics%' THEN 'Analytics'
            WHEN LOWER(Department) LIKE '%operation%' THEN 'Operations'
            ELSE 'Unknown'
        END AS Department,
        CAST(DATEDIFF(MONTH, TRY_CONVERT(DATE,HireDate,104), GETDATE()) / 12 AS NVARCHAR(4)) + ' Year ' +
        CAST(DATEDIFF(MONTH, TRY_CONVERT(DATE,HireDate,104), GETDATE()) % 12 AS NVARCHAR(4)) + ' Month' AS ExpLevel
    FROM CTE_Employee
    WHERE rn = 1;
END;
GO


-- SP: Load FactPerformance
IF OBJECT_ID('DW.sp_LoadFactPerformance','P') IS NOT NULL
    DROP PROCEDURE DW.sp_LoadFactPerformance;
GO

CREATE PROCEDURE DW.sp_LoadFactPerformance
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO DW.FactPerformance (EmpID, ReviewDate, Rating, Bonus, AnnualReview, Promoted)
    SELECT 
        TRY_CAST(EmployeeID AS INT),
        TRY_CONVERT(DATE, ReviewDate, 104),
        TRY_CAST(PerformanceScore AS INT),
        TRY_CAST(Bonus AS DECIMAL(10,2)),
        CASE 
            WHEN TRY_CAST(PerformanceScore AS INT) > 5 THEN 'Excellent'
            WHEN TRY_CAST(PerformanceScore AS INT) BETWEEN 3 AND 5 THEN 'Good'
            WHEN TRY_CAST(PerformanceScore AS INT) >= 0 AND TRY_CAST(PerformanceScore AS INT) < 3 THEN 'Average'
            ELSE 'Unknown'
        END AS AnnualReview,
        CASE WHEN Promotion = '1' THEN 'Yes' ELSE 'No' END
    FROM Staging.Performance_raw;
END;
GO


-- SP: Load DimTitle
IF OBJECT_ID('DW.sp_LoadDimTitle','P') IS NOT NULL
    DROP PROCEDURE DW.sp_LoadDimTitle;
GO

CREATE PROCEDURE DW.sp_LoadDimTitle
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO DW.DimTitle (JobTitle)
    SELECT DISTINCT JobTitle
    FROM DW.DimEmployee
    WHERE JobTitle IS NOT NULL;
END;
GO


-- SP: Load DimDate
IF OBJECT_ID('DW.sp_LoadDimDate','P') IS NOT NULL
    DROP PROCEDURE DW.sp_LoadDimDate;
GO

CREATE PROCEDURE DW.sp_LoadDimDate
AS
BEGIN
    SET NOCOUNT ON;

    WITH DateSeries AS (
        SELECT CAST('2000-01-01' AS DATE) AS d
        UNION ALL
        SELECT DATEADD(DAY, 1, d) FROM DateSeries WHERE d < '2030-12-31'
    )
    INSERT INTO DW.DimDate
    SELECT 
        CONVERT(INT, FORMAT(d, 'yyyyMMdd')) AS DateKey,
        d AS FullDate,
        DAY(d),
        MONTH(d),
        DATENAME(MONTH, d),
        DATEPART(QUARTER, d),
        YEAR(d),
        DATEPART(WEEK, d),
        DATEPART(WEEKDAY, d),
        DATENAME(WEEKDAY, d)
    FROM DateSeries
    OPTION (MAXRECURSION 0);
END;
GO


-- ===================================================
-- 6. Run ETL (Execute Stored Procedures)
-- ===================================================

EXEC DW.sp_LoadDimEmployee;
EXEC DW.sp_LoadDimTitle;
EXEC DW.sp_LoadDimDate;
EXEC DW.sp_LoadFactPerformance;
