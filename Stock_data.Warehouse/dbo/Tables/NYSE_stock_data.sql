CREATE TABLE [dbo].[NYSE_stock_data] (

	[trade_date] date NULL, 
	[closing_price] float NULL, 
	[ticker] varchar(10) NULL
);


GO
ALTER TABLE [dbo].[NYSE_stock_data] ADD CONSTRAINT FK_6013ff1e_a23d_4c28_a3c1_57f49b20ca0e FOREIGN KEY ([trade_date]) REFERENCES [dbo].[NYSE_calendar]([date_value]);
GO
ALTER TABLE [dbo].[NYSE_stock_data] ADD CONSTRAINT FK_9581e29d_7826_4bf8_a127_280a25f71fa5 FOREIGN KEY ([ticker]) REFERENCES [dbo].[ticker]([ticker]);