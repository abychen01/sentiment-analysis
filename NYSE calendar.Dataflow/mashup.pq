[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "nyse_calendar_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "date_value", DestinationColumnName = "date_value"], [SourceColumnName = "NYSE_holiday", DestinationColumnName = "NYSE_holiday"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "Month name", DestinationColumnName = "Month name"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "Day name", DestinationColumnName = "Day name"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared nyse_calendar = let
  Source = Fabric.Warehouse([]),
  #"Navigation 1" = Source{[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[warehouseId = "b5992a81-df73-43a3-bc93-57a46b5c8c08"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Schema = "dbo", Item = "nyse_calendar_2025"]}[Data],
  #"Inserted year" = Table.AddColumn(#"Navigation 3", "Year", each Date.Year([date_value]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted year", "Month", each Date.Month([date_value]), type nullable number),
  #"Inserted month name" = Table.AddColumn(#"Inserted month", "Month name", each Date.MonthName([date_value]), type nullable text),
  #"Inserted day" = Table.AddColumn(#"Inserted month name", "Day", each Date.Day([date_value]), type nullable number),
  #"Inserted day name" = Table.AddColumn(#"Inserted day", "Day name", each Date.DayOfWeekName([date_value]), type nullable text),
  #"Inserted conditional column" = Table.AddColumn(#"Inserted day name", "NYSE_holiday", each if [is_nyse_holiday] = true then true else if [Day name] = "Sunday" then true else if [Day name] = "Saturday" then true else false),
  #"Sorted rows" = Table.Sort(#"Inserted conditional column", {{"Day name", Order.Descending}}),
  #"Removed columns" = Table.RemoveColumns(#"Sorted rows", {"is_nyse_holiday"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"date_value", "NYSE_holiday", "Year", "Month", "Month name", "Day", "Day name"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"NYSE_holiday", type logical}, {"Year", Int64.Type}, {"Month", Int64.Type}, {"Day", Int64.Type}})
in
  #"Changed column type";
shared nyse_calendar_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "b5992a81-df73-43a3-bc93-57a46b5c8c08"]}[Data],
  TableNavigation = Navigation_2{[Item = "NYSE_calendar", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
