CREATE TABLE [dbo].[NYSE_calendar] (

	[date_value] date NULL, 
	[NYSE_holiday] bit NULL, 
	[Year] bigint NULL, 
	[Month] bigint NULL, 
	[Month name] varchar(8000) NULL, 
	[Day] bigint NULL, 
	[Day name] varchar(8000) NULL
);

