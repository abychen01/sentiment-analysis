[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared stock_data_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "b5992a81-df73-43a3-bc93-57a46b5c8c08"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "stock_data"]}[Data]
in
  TableNavigation;
shared latest_date = let
  Source = Fabric.Warehouse([]),
  #"Navigation 1" = Source{[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[warehouseId = "b5992a81-df73-43a3-bc93-57a46b5c8c08"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Schema = "dbo", Item = "stock_data"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 3", {"trade_date"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"trade_date"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed duplicates", {{"trade_date", type date}}),
  #"Drill down" = #"Changed column type"[trade_date],
  #"Sorted items" = List.Sort(#"Drill down", Order.Descending),
  #"Kept first items" = List.FirstN(#"Sorted items", 1)
in
  #"Kept first items";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "temp_stock_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "trade_date"], [SourceColumnName = "Close", DestinationColumnName = "closing_price"], [SourceColumnName = "ticker", DestinationColumnName = "ticker"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared temp_stock_data = let
  Source = Lakehouse.Contents(null){[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data]{[lakehouseId = "edd6afe6-a35e-4241-aca0-556447fcffbe"]}[Data],
  #"Navigation 1" = Source{[Id = "temp_stock_data", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Date", type date}}),
  #"FilteredRows" = Table.SelectRows(#"Changed column type", each [Date] > latest_date{0})

in
  #"FilteredRows";
shared temp_stock_data_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "c1bac311-bdf8-44ad-907a-6a2f4314508a"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "b5992a81-df73-43a3-bc93-57a46b5c8c08"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "stock_data"]}[Data]
in
  TableNavigation;
