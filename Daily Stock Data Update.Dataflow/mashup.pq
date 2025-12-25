[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "temp_stock_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Close", DestinationColumnName = "Close"], [SourceColumnName = "ticker", DestinationColumnName = "ticker"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared temp_stock_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a2d18893-d874-4d0f-83d6-91d2ed3d9dfa"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "05913245-8d89-4d7f-8eee-4582e03c1258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "temp_stock_data", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Date", type date}}),
  // Extracting the latest date value
  latest_date_value = Date.From(latest_date[Date]{0}),
  FilteredRows = Table.SelectRows(#"Changed column type", each [Date] > latest_date_value)
in
  FilteredRows;
shared latest_date = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a2d18893-d874-4d0f-83d6-91d2ed3d9dfa"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "05913245-8d89-4d7f-8eee-4582e03c1258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "stock_data", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"Date"}),
  #"Sorted rows" = Table.Sort(#"Removed other columns", {{"Date", Order.Descending}}),
  #"Kept top rows" = Table.FirstN(#"Sorted rows", 1)
in
  #"Kept top rows";
shared temp_stock_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fa56728c-4e8f-4cf0-b06b-130982363f52"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8c338b2f-ab3a-4354-bdc7-d89db7abf459"]}[Data],
  TableNavigation = Navigation_2{[Id = "stock_data", ItemKind = "Table"]}[Data]
in
  TableNavigation;
