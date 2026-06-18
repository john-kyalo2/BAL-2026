[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data]{[lakehouseId = "149b9454-87ac-45e9-bbc2-25b5bf3a7680"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimTeam xlsx_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Team", DestinationColumnName = "Team"], [SourceColumnName = "Logo", DestinationColumnName = "Logo"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"DimTeam xlsx" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "149b9454-87ac-45e9-bbc2-25b5bf3a7680"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DimTeam.xlsx"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Navigation 3", null, true),
  #"Removed other columns" = Table.SelectColumns(#"Imported Excel workbook", {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Removed other columns", "Data", {"Column1", "Column2"}, {"Column1", "Column2"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Team", type text}, {"Logo", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each not Text.EndsWith([Team], "Armée Patriotique RwandaiseAPR")),
  Custom = List.Accumulate(
        Table.ToRecords(Team_cleam_mapping),
        #"Filtered rows", 
        (x, y) =>
            Table.ReplaceValue(
                x,
                y[Before],
                y[After],
                Replacer.ReplaceText,
                {"Team"}
            )
    ),
  #"Appended query" = Table.Combine({Custom, #"extra team"}),
  #"Replaced value" = Table.ReplaceValue(#"Appended query", "ASC Ville de Dakar", "ASC Ville de Dakar Senegal", Replacer.ReplaceValue, {"Team"})

in
  #"Replaced value";
shared Team_cleam_mapping = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("bZFBbsJADEWvYmVTInGJUdLQloAQATaIhROmwWI0RsOkVXqj7nsDLlYzpEDabiLlP3/Pt71eRyk6SMi3MFig/UBLGKdqHg1v4EePNsN19MJl42oYEVp/hEHBjd+BenVUYfzyNBJfv+K+IDSYIjkuqWu9a+xWu2myEON/BMbathejKhJYkTEathpS3KNTq1Rsf/VQnpim7N4lK8M1lo4yW5Ir8fRhx4ItQ4m7tMQWigM7mUC6zrFEny0Lccr38gsTdlxVfMlmQO1MC7msMaeyxVip/Jztpge5Vzx4rNuDjyX/XWXQrgNoUN6cPj1VDDPtHRvNIVHeoN3iLD0/EsBNBGVrNt21EgVjsrVcKjl9eVnRw/Mbk9OxkPOtrryPg3mCe8/vFjLTaicdplRrJyucjDOx/qIdjDabbw==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Before = _t, After = _t])
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_bal_standings_snapshot_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Position", DestinationColumnName = "Position"], [SourceColumnName = "Team", DestinationColumnName = "Team"], [SourceColumnName = "GP", DestinationColumnName = "GP"], [SourceColumnName = "W", DestinationColumnName = "W"], [SourceColumnName = "L", DestinationColumnName = "L"], [SourceColumnName = "GD", DestinationColumnName = "GD"], [SourceColumnName = "%won", DestinationColumnName = "%won"], [SourceColumnName = "conference", DestinationColumnName = "conference"], [SourceColumnName = "snapshot_utc", DestinationColumnName = "snapshot_utc"], [SourceColumnName = "Pts", DestinationColumnName = "Pts"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_bal_standings_snapshot = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "149b9454-87ac-45e9-bbc2-25b5bf3a7680"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_bal_standings_snapshot", ItemKind = "Table"]}[Data],
  #"Trimmed text" = Table.TransformColumns(#"Navigation 2", {{"Team", each Text.Trim(_), type nullable text}}),
  Custom = Table.SelectRows(#"Trimmed text", each not ([Team] = "RSSB Tigers Rwanda" and [GP] = 0)),
  #"Split column by position" = Table.SplitColumn(Custom, "Team", Splitter.SplitTextByPositions({0, 3}, true), {"Team", "Team.2"}),
  #"Removed columns" = Table.RemoveColumns(#"Split column by position", {"Team.2"})
in
  #"Removed columns";
shared #"extra team" = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("JcxRDoIgAADQqzg+/GoS6STbWFtH0P6cawiIJCkChnH61noHeG0L6qa5JXclhXVJHejMKTiA0XvjLhCGEDJuF9Mve8aWF3RMw0FBlK/FzHYfN0VFGScncF7A3/T4F9nTyKvVk/iQsuDnflvxYCwa2KZWUckKazymzhOGoqDv0zG1NBAEuu4L", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Team = _t, Logo = _t])
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "BAL_PlayerProfile csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Player Name", DestinationColumnName = "Player Name"], [SourceColumnName = "Age", DestinationColumnName = "Age"], [SourceColumnName = "Position", DestinationColumnName = "Position"], [SourceColumnName = "Height", DestinationColumnName = "Height"], [SourceColumnName = "Nationality", DestinationColumnName = "Nationality"], [SourceColumnName = "Jersey No.", DestinationColumnName = "Jersey No_"], [SourceColumnName = "Team", DestinationColumnName = "Team"], [SourceColumnName = "P.Image", DestinationColumnName = "P.Image"], [SourceColumnName = "DOB", DestinationColumnName = "DOB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"BAL_PlayerProfile csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "149b9454-87ac-45e9-bbc2-25b5bf3a7680"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "BAL_PlayerProfile.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Player Name", type text}, {"Age", Int64.Type}, {"Position", type text}, {"Height", Int64.Type}, {"Nationality", type text}, {"Jersey No.", Int64.Type}, {"Team ", type text}, {"P.Image", type text}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "DOB", each try Date.FromText([#"Date of Birth "], [Format="dd/MM/yyyy"])
    otherwise try Date.FromText([#"Date of Birth "], [Format="MM/dd/yyyy"])
    otherwise try Date.FromText([#"Date of Birth "], [Format="yyyy/MM/dd"])
    otherwise null),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"DOB", type date}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Date of Birth "}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Team ", "Team"}}),
  #"Trimmed text" = Table.TransformColumns(#"Renamed columns", {{"Player Name", each Text.Trim(Text.From(_)), type nullable text}, {"Age", each Text.Trim(Text.From(_)), type nullable text}, {"Position", each Text.Trim(Text.From(_)), type nullable text}, {"Height", each Text.Trim(Text.From(_)), type nullable text}, {"Nationality", each Text.Trim(Text.From(_)), type nullable text}, {"Jersey No.", each Text.Trim(Text.From(_)), type nullable text}, {"Team", each Text.Trim(Text.From(_)), type nullable text}, {"P.Image", each Text.Trim(Text.From(_)), type nullable text}, {"DOB", each Text.Trim(Text.From(_)), type nullable text}})
in
  #"Trimmed text";
shared silver_bal_standings_snapshot_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "85fbe505-cb78-4627-a244-8339c59a2733"]}[Data],
  TableNavigation = Navigation_2{[Item = "Team Standings", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared #"DimTeam xlsx_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "BAL_Warehouse"]}[Data],
  TableNavigation = Navigation_2{[Item = "DimTeam", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared #"BAL_PlayerProfile csv_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "d3924b68-b19f-47cd-a229-e0de011cfbb5"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "85fbe505-cb78-4627-a244-8339c59a2733"]}[Data],
  TableNavigation = Navigation_2{[Item = "Player Profile", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
