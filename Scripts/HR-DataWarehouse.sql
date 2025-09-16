
-- Show store details with average sales  -- Subquery from SELECT clause

SELECT DISTINCT StoreID,Restaurant_Name,  

	( SELECT  CAST(ROUND(AVG(TotalSales),0) AS int)  FROM PROJECT.DBO.FACTJOIN AS F
	WHERE D.StoreID = F.StoreID) AS Average FROM PROJECT.DBO.FACTJOIN AS D
	

 -- Find Store that has sales greater than average value  -- Subquery from FROM clause

SELECT Restaurant_Name,Sales,Average FROM (
			SELECT Restaurant_Name,CAST(ROUND(TotalSales,0) AS int) AS Sales, 
			CAST(ROUND(AVG(TotalSales) OVER(),0) AS int) AS Average  
			FROM PROJECT.DBO.FACTJOIN ) as rw
WHERE Sales > Average ;

-- Rank Restaurant_Name based on their totalsales -- Subquery from FROM clause

SELECT Restaurant_Name , RANK() OVER(ORDER BY Sales DESC ) as RankSales from   

		(SELECT Restaurant_Name, CAST(coalesce(ROUND(SUM(TotalSales),0),0) AS int) as Sales 
		FROM  Project.DBO.FACTJOIN
		GROUP BY Restaurant_Name ) as DT  

-- Show the storeid's, names, sales, total no of orders -- Subquery from WHERE & LEFTJOIN clause


SELECT M.StoreID, M.Restaurant_Name,M.TotalSales, S.Orders
	from Project.dbo.FACTJOIN AS M   
	LEFT JOIN 
			( SELECT StoreID, COUNT(*) AS Orders 
			FROM Project.DBO.FACTJOIN  
			GROUP BY StoreID ) 
	AS S ON M.StoreID = S.StoreID


 -- Find Store that has sales greater than average value  -- Subquery from WHERE clause

 SELECT Restaurant_Name,CAST(ROUND(TotalSales,0)AS INT) as Sales,
	CAST(ROUND((SELECT AVG(TotalSales) as Average FROM Project.dbo.FACTJOIN),0) AS INT) as Average
	FROM Project.DBO.FACTJOIN
	WHERE TotalSales > (SELECT AVG(TotalSales) as Average FROM Project.dbo.FACTJOIN)
       
		
-- Find store details made during high traffic

SELECT * FROM Project.DBO.FACTJOIN
WHERE StoreID in 
( SELECT StoreID FROM Project.DBO.FACTJOIN WHERE Traffic_Level = 'High')

SELECT * FROM Project.DBO.FACTJOIN

-- find sales traffic level where medium is greater than any high leve -- > ANY vs > ALL

SELECT Restaurant_Name, Traffic_Level , TotalSales FROM Project.dbo.FACTJOIN
WHERE Traffic_Level = 'Low' and TotalSales > ANY
(SELECT   TotalSales FROM Project.dbo.FACTJOIN WHERE Traffic_Level = 'High')

SELECT Restaurant_Name, Traffic_Level , TotalSales FROM Project.dbo.FACTJOIN
WHERE Traffic_Level = 'Low' and TotalSales > ALL
(SELECT   TotalSales FROM Project.dbo.FACTJOIN WHERE Traffic_Level = 'High')


-- Find Storeid who match both storid and traffic level

SELECT * FROM Project.DBO.FACTJOIN
WHERE StoreID IN ( SELECT StoreID FROM Project.DBO.FACTJOIN WHERE Traffic_Level = 'High')
AND Traffic_Level = 'High'


-- Identify or remove duplicates 

SELECT StoreID,Count(*) as Total FROM Project.DBO.FACTJOIN
GROUP BY StoreID
Having Count(*) = 1  -- Remove duplicates

SELECT StoreID,Count(*) as Total FROM Project.DBO.FACTJOIN
GROUP BY StoreID
Having Count(*) > 1  -- Identify duplicates


-- find out TopN

SELECT * FROM 

	( SELECT  Restaurant_Name,TotalSales, ROW_NUMBER() OVER (ORDER BY TotalSales DESC) AS ROWRANK -- SubQuery

FROM Project.DBO.FACTJOIN ) RANKED
WHERE ROWRANK < = 1


--RankingFunctions

SELECT Restaurant_Name, Traffic_Level, TotalSales, ROW_NUMBER() OVER (ORDER BY TotalSales DESC ) AS ROWRANKED,
RANK() OVER (ORDER BY TotalSales DESC ) AS RANKED, DENSE_RANK() OVER (ORDER BY TotalSales DESC ) AS DENSE

FROM Project.DBO.FACTJOIN