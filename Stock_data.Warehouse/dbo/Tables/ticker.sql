CREATE TABLE [dbo].[ticker] (

	[ticker] varchar(10) NULL
);


GO
ALTER TABLE [dbo].[ticker] ADD CONSTRAINT UQ_9f35398e_cb59_4231_b7b3_9d75cec3eb8b unique NONCLUSTERED ([ticker]);