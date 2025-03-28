CREATE TABLE [dbo].[NYSE_calendar] (

	[date_value] date NULL, 
	[NYSE_holiday] bit NULL, 
	[Year] bigint NULL, 
	[Month] bigint NULL, 
	[Month name] varchar(8000) NULL, 
	[Day] bigint NULL, 
	[Day name] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[NYSE_calendar] ADD CONSTRAINT UQ_ef84f6e0_5c03_4c7c_8064_a878edd34134 unique NONCLUSTERED ([date_value]);