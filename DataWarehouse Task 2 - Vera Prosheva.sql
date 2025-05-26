--Add columns ValidFrom, ValidTo, ModifiedDate
ALTER TABLE dimension.Account ADD
	ValidFrom date,
	ValidTo date,
	ModifiedDate datetime

update dimension.Account
set ValidFrom = '1753-01-01',
	ValidTo = '9999-12-31'

--Update dbo.Account to check procedure
select * from BrainsterDB.dbo.Account where Id = 1
select * from dimension.Account where AccountID = 1

update BrainsterDB.dbo.Account
set AccountNumber = '410123456789014', --Type 1
    AllowedOverdraft = 286000.00 --Type 2
where ID = 1
	
--Account - [integration].[InsertDimensionAccount_Incremental]
--SCD Type 1: AccountNumber
--SCD Type 2: AllowedOverdraft

CREATE PROCEDURE integration.InsertDimensionAccount_Incremental (@Workday date)
AS
BEGIN

	DECLARE @MaxDate date = '9999-12-31'

	--Temp table
	create table #AccountChanges
	(
		[AccountID] int not null, 
		[AllowedOverdraft] decimal(18,2) null
	)

	--Insert into temp table
	insert into #AccountChanges ([AccountID], [AllowedOverdraft])
	select ab.ID as AccountID,
		   ab.AllowedOverdraft
	from dimension.Account as a
	inner join BrainsterDB.dbo.Account as ab on ab.ID = a.AccountID
	where a.ValidFrom <= @Workday and @Workday <= a.ValidTo
	and a.ValidTo = @MaxDate
	and (
		  isnull(a.AllowedOverdraft, 0) <> isnull(ab.AllowedOverdraft, 0)
	)

	--Update SCD Type 1
	update a
	set a.AccountNumber = ab.AccountNumber,
		a.ModifiedDate = GETDATE()
	from dimension.Account as a
	inner join BrainsterDB.dbo.Account as ab on ab.ID = a.AccountID
	where a.ValidFrom <= @Workday and @Workday <= a.ValidTo
	and a.ValidTo = @MaxDate
	and HASHBYTES('SHA1', isnull(a.AccountNumber, '')) 
	 <> HASHBYTES('SHA1', isnull(ab.AccountNumber, '')) 

	--Update SCD Type 2 ValidTo
	update a
	set ValidTo = @Workday,
		ModifiedDate = GETDATE()
	from dimension.Account as a
	inner join #AccountChanges as ac on ac.AccountID = a.AccountID 
	where a.ValidFrom <= @Workday and @Workday <= a.ValidTo
	and a.ValidTo = @MaxDate

	--SCD Type 2 insert new values from temp
	insert into dimension.Account ([AccountID], [AccountNumber], [AllowedOverdraft], [ValidFrom], [ValidTo], [ModifiedDate])
	select [AccountID],  [AccountNumber], [AllowedOverdraft],
		   @Workday as [ValidFrom], 
		   @MaxDate as [ValidTo], 
		   GETDATE() as [ModifiedDate]
	from #AccountChanges

	--Insert new records (Accounts)
	insert into dimension.Account([AccountID], [AccountNumber], [AllowedOverdraft], [ValidFrom], [ValidTo], [ModifiedDate])
	select ab.ID as [AccountID], ab.[AccountNumber], ab.[AllowedOverdraft],
		   @Workday as [ValidFrom], 
		   @MaxDate as [ValidTo], 
		   GETDATE() as [ModifiedDate] 
	from BrainsterDB.dbo.Account as ab
	where NOT EXISTS 
		(
		select * from dimension.Account as a
		where ab.ID = a.AccountId and a.ValidTo = @MaxDate
		)

END
GO

EXEC [integration].[InsertDimensionAccount_Incremental] '2019-04-30'


--AccountDetails - [integration].[InsertFactAccountDetails_Incremental]

--LastAggregation table

CREATE TABLE integration.LastAggregation
(
	FactName nvarchar(50)
,	LastAggregation date
)
GO

insert into integration.LastAggregation (FactName, LastAggregation)
select 'fact.AccountDetails' as FactName, max(DateKey) as LastAggregation from fact.AccountDetails
GO

--Procedure

CREATE PROCEDURE integration.InsertFactAccountDetails_Incremental (@Workday date)
AS
BEGIN

	--Get Last aggregation date
	DECLARE @LastAggregation date

	select @LastAggregation = LastAggregation 
	from integration.LastAggregation 
	where FactName = 'fact.AccountDetails'

	--Create temp table
	CREATE TABLE #AccountDetailsChanges
	(
		[CustomerKey] [int] NOT NULL,
		[CurrencyKey] [int] NOT NULL,
		[EmployeeKey] [int] NOT NULL,
		[AccountKey] [int] NOT NULL,
		[DateKey] [date] NOT NULL,
		[CurrentBalance] [decimal](18, 2) NULL,
		[InflowTransactionsQuantity] [int] NOT NULL,
		[InflowAmount] [decimal](18, 6) NOT NULL,
		[OutflowTransactionsQuantity] [int] NOT NULL,
		[OutflowAmount] [decimal](18, 6) NOT NULL,
		[OutflowTransactionsQuantityATM] [int] NOT NULL,
		[OutflowAmountATM] [decimal](18, 6) NOT NULL
	)

	--Insert new transactions in temp
	;with cte as (
		select a.CustomerId, a.CurrencyId, a.EmployeeId, a.ID as AccountKey, ad.TransactionDate, a.AccountNumber, a.AllowedOverdraft,
			   a.Id as AccountID, d.FirstDayOfMonth, d.LastDayOfMonth,
			   ROW_NUMBER() OVER (PARTITION BY a.ID, d.LastDayOfMonth ORDER BY ad.TransactionDate) as RN
		from BrainsterDB.dbo.Account as a
			 INNER JOIN BrainsterDB.dbo.AccountDetails as ad ON a.Id = ad.AccountId
			 INNER JOIN dimension.Date as d ON ad.TransactionDate = d.DateKey
		where @LastAggregation < ad.TransactionDate and ad.TransactionDate <= @Workday
	)
	insert into #AccountDetailsChanges ([CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	select dc.CustomerKey, dcu.CurrencyKey, de.EmployeeKey, da.AccountKey, cte.LastDayOfMonth as DateKey,
		(
			select
				SUM(ad.Amount)
			from
				BrainsterDB.dbo.AccountDetails as ad
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate <= cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
		) as CurrentBalance,
		(
			select
				COUNT(ad.Amount)
			from
				BrainsterDB.dbo.AccountDetails as ad
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount > 0
			and ad.TransactionDate <= @Workday
		) as [InflowTransactionsQuantity],
		(
			select
				ISNULL(SUM(ad.Amount), 0)
			from
				BrainsterDB.dbo.AccountDetails as ad
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount > 0
			and ad.TransactionDate <= @Workday
		) as [InflowAmount],
		(
			select
				COUNT(ad.Amount)
			from
				BrainsterDB.dbo.AccountDetails as ad
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount < 0
			and ad.TransactionDate <= @Workday
		) as [OutflowTransactionsQuantity],
		(
			select
				ISNULL(SUM(ad.Amount), 0)
			from
				BrainsterDB.dbo.AccountDetails as ad
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount < 0
			and ad.TransactionDate <= @Workday
		) as [OutflowAmount],
		(
			select
				COUNT(ad.Amount)
			from
				BrainsterDB.dbo.AccountDetails as ad
				INNER JOIN BrainsterDB.dbo.[Location] as l ON ad.LocationId = l.ID
				INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId = lt.Id
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount < 0
			and lt.[Name] = 'ATM'
			and ad.TransactionDate <= @Workday
		) as [OutflowTransactionsQuantityATM],
		(
			select
				ISNULL(SUM(ad.Amount), 0)
			from
				BrainsterDB.dbo.AccountDetails as ad
				INNER JOIN BrainsterDB.dbo.[Location] as l ON ad.LocationId = l.ID
				INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId = lt.Id
			where
				ad.AccountId = cte.AccountID
			and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
			and ad.Amount < 0
			and lt.[Name] = 'ATM'
			and ad.TransactionDate <= @Workday
		) as [OutflowAmountATM]
	from cte
		LEFT OUTER JOIN dimension.Customer as dc ON cte.CustomerId = dc.CustomerID and dc.ValidFrom <= @Workday and @Workday < dc.ValidTo
		LEFT OUTER JOIN dimension.Currency as dcu ON cte.CurrencyId = dcu.CurrencyID and dcu.ValidFrom <= @Workday and @Workday < dcu.ValidTo
		LEFT OUTER JOIN dimension.Employee as de ON cte.EmployeeId = de.EmployeeID and de.ValidFrom <= @Workday and @Workday < de.ValidTo
		LEFT OUTER JOIN dimension.Account as da ON cte.AccountID = da.AccountID and da.ValidFrom <= @Workday and @Workday < da.ValidTo
	where RN = 1
	order by cte.AccountID, cte.LastDayOfMonth

	--delete transactions from DWH that exist in temp
	delete fa
	from fact.AccountDetails as fa
	where exists (
			select * from #AccountDetailsChanges as adc
			where
				adc.CustomerKey = fa.CustomerKey
			and adc.CurrencyKey = fa.CurrencyKey
			and adc.EmployeeKey = fa.EmployeeKey
			and adc.AccountKey = fa.AccountKey
			and adc.DateKey = fa.DateKey
		)

	-- insert all transactions from temp into DWH
	insert into fact.AccountDetails([CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	select [CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM]
	from #AccountDetailsChanges

	--Update LastAggregation
	update integration.LastAggregation
	set LastAggregation = @Workday
	where FactName = 'fact.AccountDetails'

END
GO

