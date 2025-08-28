[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "temp_stock_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Close", DestinationColumnName = "Close"], [SourceColumnName = "ticker", DestinationColumnName = "ticker"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared temp_stock_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "1682a0d7-4d6c-47fd-88f0-3cb543c163d6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "b3b54ba2-cc81-4a95-b001-84731590108e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "temp_stock_data", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Date", type date}}),
  // Extracting the latest date value
  latest_date_value = Date.From(latest_date[Date]{0}),
  FilteredRows = Table.SelectRows(#"Changed column type", each [Date] > latest_date_value)
in
  FilteredRows;
shared latest_date = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "1682a0d7-4d6c-47fd-88f0-3cb543c163d6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "b3b54ba2-cc81-4a95-b001-84731590108e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "stock_data", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"Date"}),
  #"Sorted rows" = Table.Sort(#"Removed other columns", {{"Date", Order.Descending}}),
  #"Kept top rows" = Table.FirstN(#"Sorted rows", 1)
in
  #"Kept top rows";
shared temp_stock_data_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "1682a0d7-4d6c-47fd-88f0-3cb543c163d6"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "b3b54ba2-cc81-4a95-b001-84731590108e"]}[Data],
  TableNavigation = Navigation_2{[Id = "stock_data", ItemKind = "Table"]}[Data]
in
  TableNavigation;
