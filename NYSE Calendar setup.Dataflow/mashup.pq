[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "1682a0d7-4d6c-47fd-88f0-3cb543c163d6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "b3b54ba2-cc81-4a95-b001-84731590108e"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NYSE_calendar_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Quarter", DestinationColumnName = "Quarter"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "Week of year", DestinationColumnName = "Week of year"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "Day name", DestinationColumnName = "Day name"], [SourceColumnName = "NYSE_holiday", DestinationColumnName = "NYSE_holiday"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NYSE_calendar = let
    Source = List.Dates(#date(2022,1,1), 1460, #duration(1,0,0,0)),
    #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Changed column type" = Table.TransformColumnTypes(#"Converted to table", {{"Column1", type date}}),
    #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Column1", "Date"}}),
    #"Inserted year" = Table.AddColumn(#"Renamed columns", "Year", each Date.Year([Date]), type number),
    #"Inserted month" = Table.AddColumn(#"Inserted year", "Month", each Date.Month([Date]), type number),
    #"Inserted quarter" = Table.AddColumn(#"Inserted month", "Quarter", each Date.QuarterOfYear([Date]), type number),
    #"Inserted day" = Table.AddColumn(#"Inserted quarter", "Day", each Date.Day([Date]), type number),
    #"Inserted day name" = Table.AddColumn(#"Inserted day", "Day name", each Date.DayOfWeekName([Date]), type text),
    #"Inserted week of year" = Table.AddColumn(#"Inserted day name", "Week of year", each Date.WeekOfYear([Date]), type number),
    #"Reordered columns" = Table.ReorderColumns(#"Inserted week of year", {"Date", "Year", "Quarter", "Month", "Week of year", "Day", "Day name"}),

    HolidayList = {
        #date(2022,1,17), #date(2022,2,21), #date(2022,4,15), #date(2022,5,30), #date(2022,6,20),
        #date(2022,7,4), #date(2022,9,5), #date(2022,11,24), #date(2022,12,26),
        #date(2023,1,2), #date(2023,1,16), #date(2023,2,20), #date(2023,4,7), #date(2023,5,29),
        #date(2023,6,19), #date(2023,7,4), #date(2023,9,4), #date(2023,11,23), #date(2023,12,25),
        #date(2024,1,1), #date(2024,1,15), #date(2024,2,19), #date(2024,3,29), #date(2024,5,27),
        #date(2024,6,19), #date(2024,7,4), #date(2024,9,2), #date(2024,11,28), #date(2024,12,25),
        #date(2025,1,1), #date(2025,1,20), #date(2025,2,17), #date(2025,4,18), #date(2025,5,26),
        #date(2025,6,19), #date(2025,7,4), #date(2025,9,1), #date(2025,11,27), #date(2025,12,25)
    },

    // Add single NYSE_holiday column based on weekends OR holiday list
    AddNYSEHolidayFlag = Table.AddColumn(#"Reordered columns", "NYSE_holiday", each 
        Date.DayOfWeek([Date]) = 0 or Date.DayOfWeek([Date]) = 6 or List.Contains(HolidayList, [Date]), type logical
    ),
  #"Changed column type 1" = Table.TransformColumnTypes(AddNYSEHolidayFlag, {{"Year", Int64.Type}, {"Quarter", Int64.Type}, {"Month", Int64.Type}, {"Week of year", Int64.Type}, {"Day", Int64.Type}})

in
    #"Changed column type 1";
shared NYSE_calendar_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "a2d18893-d874-4d0f-83d6-91d2ed3d9dfa"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "2593cb7f-7eac-4662-8cf4-777be24bd84d"]}[Data],
  TableNavigation = Navigation_2{[Item = "NYSE_calendar", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
