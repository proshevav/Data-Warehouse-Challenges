--Create database BrainsterDW2
CREATE DATABASE BrainsterDW2
GO

use BrainsterDW2
go

--Create schemas
create schema dimension
go
create schema fact
go
create schema integration
go

--Create tables
CREATE TABLE dimension.Customer (
CustomerKey int IDENTITY(1,1) not null,
CustomerID int not null,
FirstName nvarchar(100) not null,
LastName nvarchar(100) not null,
Gender nchar(1) null,
NationalIdNumber nvarchar(15) null,
DateOfBirth date null,
RegionName nvarchar(100) null,
PhoneNumber nvarchar(20) null,
IsActive bit not null,
CityName nvarchar(100) null,
Region nvarchar(100) null,
Population decimal(10,3) null,
	CONSTRAINT PK_Customer PRIMARY KEY CLUSTERED
	(CustomerKey ASC)
)
go

CREATE TABLE dimension.Currency (
CurrencyKey int IDENTITY(1,1) not null,
CurrencyID int not null,
Code nvarchar(5) null,
Name nvarchar(100) null,
ShortName nvarchar(20) null,
CountryName nvarchar(100) null,
	CONSTRAINT PK_Currency PRIMARY KEY CLUSTERED 
	(CurrencyKey ASC)
)
GO

CREATE TABLE dimension.Employee (
EmployeeKey int IDENTITY(1,1) not null,
EmployeeID int not null,
FirstName nvarchar(100) not null,
LastName nvarchar(100) not null,
NationalIdNumber nvarchar(15) null,
JobTitle nvarchar(50) null,
DateOfBirth date null,
MaritalStatus nchar(1) null,
Gender nchar(1) null,
HireDate date null,
CityName nvarchar(100) null,
Region nvarchar(100) null,
Population decimal(10,3) null,
	CONSTRAINT PK_Employee PRIMARY KEY CLUSTERED 
	(EmployeeKey ASC)
)
GO

CREATE TABLE dimension.Account (
AccountKey int IDENTITY(1,1) not null,
AccountID int not null,
AccountNumber nvarchar(20) null,
AllowedOverdraft  decimal(18,2) null,
	CONSTRAINT PK_Account PRIMARY KEY CLUSTERED 
	(AccountKey ASC)
)
GO

CREATE TABLE fact.AccountDetails (
AccountDetailsKey bigint IDENTITY(1,1) not null,
CustomerKey int not null,
CurrencyKey int not null,
EmployeeKey int not null,
AccountKey int not null,
DateKey date not null,
CurrentBalance decimal(18,2) null,
InflowTransactionsQuantity int not null,
InflowAmount decimal(18,6) not null,
OutflowTransactionsQuantity int not null,
OutflowAmount decimal(18,6) not null,
OutflowTransactionsQuantityATM int not null,
OutflowAmountATM decimal(18,6) not null,
	CONSTRAINT PK_AccountDetails PRIMARY KEY CLUSTERED 
	(AccountDetailsKey ASC)
)
GO

CREATE TABLE [dimension].[Date]
(
	[DateKey] Date NOT NULL
,	[Day] TINYINT NOT NULL
,	DaySuffix CHAR(2) NOT NULL
,	[Weekday] TINYINT NOT NULL
,	WeekDayName VARCHAR(10) NOT NULL
,	IsWeekend BIT NOT NULL
,	IsHoliday BIT NOT NULL
,	HolidayText VARCHAR(64) SPARSE
,	DOWInMonth TINYINT NOT NULL
,	[DayOfYear] SMALLINT NOT NULL
,	WeekOfMonth TINYINT NOT NULL
,	WeekOfYear TINYINT NOT NULL
,	ISOWeekOfYear TINYINT NOT NULL
,	[Month] TINYINT NOT NULL
,	[MonthName] VARCHAR(10) NOT NULL
,	[Quarter] TINYINT NOT NULL
,	QuarterName VARCHAR(6) NOT NULL
,	[Year] INT NOT NULL
,	MMYYYY CHAR(6) NOT NULL
,	MonthYear CHAR(7) NOT NULL
,	FirstDayOfMonth DATE NOT NULL
,	LastDayOfMonth DATE NOT NULL
,	FirstDayOfQuarter DATE NOT NULL
,	LastDayOfQuarter DATE NOT NULL
,	FirstDayOfYear DATE NOT NULL
,	LastDayOfYear DATE NOT NULL
,	FirstDayOfNextMonth DATE NOT NULL
,	FirstDayOfNextYear DATE NOT NULL
,	CONSTRAINT [PK_Date] PRIMARY KEY CLUSTERED 
	(
		[DateKey] ASC
	)
)
GO

--Create FKs
ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Customer_AccountDetails FOREIGN KEY (CustomerKey)
REFERENCES dimension.Customer (CustomerKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Currency_AccountDetails FOREIGN KEY (CurrencyKey)
REFERENCES dimension.Currency (CurrencyKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Employee_AccountDetails FOREIGN KEY (EmployeeKey)
REFERENCES dimension.Employee (EmployeeKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Account_AccountDetails FOREIGN KEY (AccountKey)
REFERENCES dimension.Account (AccountKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Date_AccountDetails FOREIGN KEY (DateKey)
REFERENCES dimension.[Date] (DateKey)
GO

----Full Initial Load BrainsterDB -> BrainsterDW2

--Customer

CREATE PROCEDURE integration.InsertDimensionCustomer
AS
BEGIN 
	insert into dimension.Customer ([CustomerID], [FirstName], [LastName], [Gender], [NationalIdNumber], [DateOfBirth], [PhoneNumber], [IsActive], [CityName], [Region], [Population])
	select c.ID as CustomerID,
			c.FirstName,
			c.LastName,
			c.Gender,
			c.NationalIDNumber,
			c.DateOfBirth,
			c.PhoneNumber,
			c.isActive,
			ct.Name as CityName,
			ct.Region,
			ct.Population
	from BrainsterDB.dbo.Customer as c
	left outer join BrainsterDB.dbo.City as ct on ct.Id = c.CityId
	order by c.ID
END 
GO

EXEC [integration].[InsertDimensionCustomer]

select * from dimension.Customer

--Currency

CREATE PROCEDURE integration.InsertDimensionCurrency 
AS 
BEGIN
	insert into dimension.Currency ([CurrencyID], [Code], [Name], [ShortName], [CountryName])
	select c.ID as CurrencyID,
			c.Code,
			c.Name,
			c.ShortName,
			c.CountryName
	from BrainsterDB.dbo.Currency as c
	order by c.ID
END 
GO

EXEC [integration].[InsertDimensionCurrency]

select * from dimension.Currency

--Employee

CREATE PROCEDURE integration.InsertDimensionEmployee
AS
BEGIN
	insert into dimension.Employee ([EmployeeID], [FirstName], [LastName], [NationalIdNumber], [JobTitle], [DateOfBirth], [MaritalStatus], [Gender], [HireDate], [CityName], [Region], [Population])
	select e.ID as EmployeeID,
			e.FirstName,
			e.LastName,
			e.NationalIDNumber,
			e.JobTitle,
			e.DateOfBirth,
			e.MaritalStatus,
			e.Gender,
			e.HireDate,
			ct.Name as CityName,
			ct.Region,
			ct.Population
	from BrainsterDB.dbo.Employee as e
	left outer join BrainsterDB.dbo.City as ct on ct.Id = e.CityId
	order by e.ID
END 
GO

EXEC [integration].[InsertDimensionEmployee]

select * from dimension.Employee

--Date

CREATE OR ALTER PROCEDURE [integration].[GenerateDimensionDate]
AS
BEGIN
	DECLARE
		@StartDate DATE = '2000-01-01'
	,	@NumberOfYears INT = 30
	,	@CutoffDate DATE;
	SET @CutoffDate = DATEADD(YEAR, @NumberOfYears, @StartDate);

	-- prevent set or regional settings from interfering with 
	-- interpretation of dates / literals
	SET DATEFIRST 7;
	SET DATEFORMAT mdy;
	SET LANGUAGE US_ENGLISH;

	-- this is just a holding table for intermediate calculations:
	CREATE TABLE #dim
	(
		[Date]       DATE        NOT NULL, 
		[day]        AS DATEPART(DAY,      [date]),
		[month]      AS DATEPART(MONTH,    [date]),
		FirstOfMonth AS CONVERT(DATE, DATEADD(MONTH, DATEDIFF(MONTH, 0, [date]), 0)),
		[MonthName]  AS DATENAME(MONTH,    [date]),
		[week]       AS DATEPART(WEEK,     [date]),
		[ISOweek]    AS DATEPART(ISO_WEEK, [date]),
		[DayOfWeek]  AS DATEPART(WEEKDAY,  [date]),
		[quarter]    AS DATEPART(QUARTER,  [date]),
		[year]       AS DATEPART(YEAR,     [date]),
		FirstOfYear  AS CONVERT(DATE, DATEADD(YEAR,  DATEDIFF(YEAR,  0, [date]), 0)),
		Style112     AS CONVERT(CHAR(8),   [date], 112),
		Style101     AS CONVERT(CHAR(10),  [date], 101)
	);

	-- use the catalog views to generate as many rows as we need
	INSERT INTO #dim ([date]) 
	SELECT
		DATEADD(DAY, rn - 1, @StartDate) as [date]
	FROM 
	(
		SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
			rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
		FROM
			-- on my system this would support > 5 million days
			sys.all_objects AS s1
			CROSS JOIN sys.all_objects AS s2
		ORDER BY
			s1.[object_id]
	) AS x;
	-- select * from #dim

	INSERT dimension.[Date] ([DateKey], [Day], [DaySuffix], [Weekday], [WeekDayName], [IsWeekend], [IsHoliday], [HolidayText], [DOWInMonth], [DayOfYear], [WeekOfMonth], [WeekOfYear], [ISOWeekOfYear], [Month], [MonthName], [Quarter], [QuarterName], [Year], [MMYYYY], [MonthYear], [FirstDayOfMonth], [LastDayOfMonth], [FirstDayOfQuarter], [LastDayOfQuarter], [FirstDayOfYear], [LastDayOfYear], [FirstDayOfNextMonth], [FirstDayOfNextYear])
	SELECT
		--DateKey     = CONVERT(INT, Style112),
		[DateKey]        = [date],
		[Day]         = CONVERT(TINYINT, [day]),
		DaySuffix     = CONVERT(CHAR(2), CASE WHEN [day] / 10 = 1 THEN 'th' ELSE 
						CASE RIGHT([day], 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
						WHEN '3' THEN 'rd' ELSE 'th' END END),
		[Weekday]     = CONVERT(TINYINT, [DayOfWeek]),
		[WeekDayName] = CONVERT(VARCHAR(10), DATENAME(WEEKDAY, [date])),
		[IsWeekend]   = CONVERT(BIT, CASE WHEN [DayOfWeek] IN (1,7) THEN 1 ELSE 0 END),
		[IsHoliday]   = CONVERT(BIT, 0),
		HolidayText   = CONVERT(VARCHAR(64), NULL),
		[DOWInMonth]  = CONVERT(TINYINT, ROW_NUMBER() OVER 
						(PARTITION BY FirstOfMonth, [DayOfWeek] ORDER BY [date])),
		[DayOfYear]   = CONVERT(SMALLINT, DATEPART(DAYOFYEAR, [date])),
		WeekOfMonth   = CONVERT(TINYINT, DENSE_RANK() OVER 
						(PARTITION BY [year], [month] ORDER BY [week])),
		WeekOfYear    = CONVERT(TINYINT, [week]),
		ISOWeekOfYear = CONVERT(TINYINT, ISOWeek),
		[Month]       = CONVERT(TINYINT, [month]),
		[MonthName]   = CONVERT(VARCHAR(10), [MonthName]),
		[Quarter]     = CONVERT(TINYINT, [quarter]),
		QuarterName   = CONVERT(VARCHAR(6), CASE [quarter] WHEN 1 THEN 'First' 
						WHEN 2 THEN 'Second' WHEN 3 THEN 'Third' WHEN 4 THEN 'Fourth' END), 
		[Year]        = [year],
		MMYYYY        = CONVERT(CHAR(6), LEFT(Style101, 2)    + LEFT(Style112, 4)),
		MonthYear     = CONVERT(CHAR(7), LEFT([MonthName], 3) + LEFT(Style112, 4)),
		FirstDayOfMonth     = FirstOfMonth,
		LastDayOfMonth      = MAX([date]) OVER (PARTITION BY [year], [month]),
		FirstDayOfQuarter   = MIN([date]) OVER (PARTITION BY [year], [quarter]),
		LastDayOfQuarter    = MAX([date]) OVER (PARTITION BY [year], [quarter]),
		FirstDayOfYear      = FirstOfYear,
		LastDayOfYear       = MAX([date]) OVER (PARTITION BY [year]),
		FirstDayOfNextMonth = DATEADD(MONTH, 1, FirstOfMonth),
		FirstDayOfNextYear  = DATEADD(YEAR,  1, FirstOfYear)
	FROM #dim
END
GO

delete from dimension.Date
GO
EXEC [integration].[GenerateDimensionDate]
GO

select * from dimension.Date

--Account

CREATE PROCEDURE integration.InsertDimensionAccount
AS
BEGIN
	insert into dimension.Account ([AccountID], [AccountNumber], [AllowedOverdraft])
	select a.ID as AccountID,
			a.AccountNumber,
			a.AllowedOverdraft
	from BrainsterDB.dbo.Account as a
	order by a.ID
END 
GO

EXEC [integration].[InsertDimensionAccount]

select * from dimension.Account

--Fact.AccountDetails

CREATE PROCEDURE integration.InsertFactAccountDetails
AS
BEGIN
	with cte as (
	select a.CustomerId,
			a.CurrencyId,
			a.EmployeeId,
			ad.TransactionDate,
			a.ID as AccountId,
			d.FirstDayOfMonth,
			d.LastDayOfMonth,
			ROW_NUMBER() OVER(partition by a.ID, d.LastDayOfMonth order by ad.TransactionDate) as rn
	from BrainsterDB.dbo.Account as a
	inner join BrainsterDB.dbo.AccountDetails as ad on ad.AccountId = a.ID
	inner join dimension.Date as d on d.DateKey = ad.TransactionDate
	)

	insert into fact.AccountDetails ([CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	select dc.CustomerKey, 
			dcu.CurrencyKey, 
			de.EmployeeKey, 
			a.AccountKey,
			cte.LastDayOfMonth as DateKey, 
			(
				select sum(ad.Amount) 
				from BrainsterDB.dbo.AccountDetails as ad 
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate <= cte.LastDayOfMonth
			) as CurrentBalance,
			(
				select count(ad.ID)
				from BrainsterDB.dbo.AccountDetails as ad
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount > 0
			) as InflowTransactionsQuantity,
			(
				select ISNULL(sum(ad.Amount), 0)
				from BrainsterDB.dbo.AccountDetails as ad
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount > 0
			) as InflowAmount,
				(
				select count(ad.ID)
				from BrainsterDB.dbo.AccountDetails as ad
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount < 0
			) as OutflowTransactionsQuantity,
			(
				select ISNULL(sum(ad.Amount), 0)
				from BrainsterDB.dbo.AccountDetails as ad
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount < 0
			) as OutflowAmount,
						(
				select count(ad.ID)
				from BrainsterDB.dbo.AccountDetails as ad
				inner join BrainsterDB.dbo.Location as l on l.ID = ad.LocationId
				inner join BrainsterDB.dbo.LocationType as lt on lt.ID = l.LocationTypeId
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount < 0
				and lt.Name = 'ATM'
			) as OutflowTransactionsQuantityATM,
			(
				select ISNULL(sum(ad.Amount), 0)
				from BrainsterDB.dbo.AccountDetails as ad
				inner join BrainsterDB.dbo.Location as l on l.ID = ad.LocationId
				inner join BrainsterDB.dbo.LocationType as lt on lt.ID = l.LocationTypeId
				where ad.AccountId = cte.AccountId 
				and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth 
				and ad.Amount < 0
				and lt.Name = 'ATM'
			) as OutflowAmountATM
	from cte 
	left outer join dimension.Customer as dc on cte.CustomerId = dc.CustomerID
	left outer join dimension.Currency as dcu on cte.CurrencyId = dcu.CurrencyID
	left outer join dimension.Employee as de on cte.EmployeeId = de.EmployeeID
	left outer join dimension.Account as a on cte.AccountId = a.AccountID
	where rn = 1
	order by cte.AccountId, cte.LastDayOfMonth
END 
GO

EXEC [integration].[InsertFactAccountDetails]

select * from fact.AccountDetails

--Final check
select * from dimension.Customer
select * from dimension.Currency
select * from dimension.Employee
select * from dimension.Account
select * from dimension.Date
select * from fact.AccountDetails

