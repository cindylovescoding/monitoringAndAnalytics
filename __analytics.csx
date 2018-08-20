 using System.Linq;
 private static string GetTableName(string dataSource, string timeRange)
{
    var dbName = "wawsprod";

    switch (dataSource)
    {
        case "1":
            return $@"cluster('wawscus').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})";
        case "2":
            return $@"union withsource = SourceTable 
            (cluster('wawseus').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})), 
            (cluster('wawseas').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})), 
            (cluster('wawsneu').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange}))";
        // Return tables from both internal and external portals by default
        default:
            return $@"union withsource = SourceTable 
            (cluster('wawscus').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})), 
            (cluster('wawseus').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})), 
            (cluster('wawseas').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange})), 
            (cluster('wawsneu').database('{dbName}').DiagnosticRole | where TIMESTAMP >= ago({timeRange}))";
    }
}

private static string GetUniqueSubscriptionQuery(string detectorId, string dataSource, string timeRange)
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '63002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
        | summarize count() by SubscriptionId
    | count
    ";
}

private static string GetUniqueResourceQuery(string detectorId, string dataSource, string timeRange)
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '63002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
    | extend s1 = split(Address, '/providers/')
    | extend  s = split(s1[1], '/')
    | extend resourceName = s[2]
    | project tostring(resourceName) 
    | summarize count() by resourceName
    | count
    ";
}

private static string GetUserandResourceRangeQuery(string detectorId, string dataSource, string timeRange, string timeGrain = "5m")
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '63002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
        | extend s1 = split(Address, '/providers/')
    | extend  s = split(s1[1], '/')
    | extend resourceName = tostring(s[2])
    | summarize Resources = dcount(resourceName), Subscriptions = dcount(SubscriptionId) by bin(PreciseTimeStamp, {timeGrain})
    ";
}


private static string GetInsightsQuery(string detectorId, string timeRange)
{
    return
    $@" customEvents 
    | where timestamp >= ago({timeRange})
    | where name contains 'InsightsTitleClicked' 
    | where customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.IsExpanded == 'true'
    | summarize HitCount = count() by tostring(customDimensions.Title)
    | project  InsightsTitle = customDimensions_Title, HitCount 
    | sort by HitCount desc  
    ";
}

private static string GetCustomEventsInsightsCount(string detectorId, string timeRange)
{
    return
    $@" customEvents 
    | where timestamp >= ago({timeRange}) 
    | where name contains 'InsightsTitleClicked' 
    | where customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.IsExpanded == 'true'
    | summarize HitCount = count() by tostring(customDimensions.Title)
    | summarize count()
    ";
}

private static string GetAllCustomEventsQuery(string dataSource, DataTable externalInsightsTable, DataTable internalInsightsTable)
{
    Dictionary<string, long> allhash = new Dictionary<string, long>();

    if (dataSource != "2")
    {
        for (int i = 0; i < internalInsightsTable.Rows.Count; i++)
        {
            if (allhash.ContainsKey(internalInsightsTable.Rows[i]["InsightsTitle"].ToString()))
            {
                allhash[internalInsightsTable.Rows[i]["InsightsTitle"].ToString()] += Convert.ToInt64(internalInsightsTable.Rows[i]["HitCount"]);
            }
            else
            {
                allhash[internalInsightsTable.Rows[i]["InsightsTitle"].ToString()] = Convert.ToInt64(internalInsightsTable.Rows[i]["HitCount"]);
            }
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < externalInsightsTable.Rows.Count; i++)
        {
            if (allhash.ContainsKey(externalInsightsTable.Rows[i]["InsightsTitle"].ToString()))
            {
                allhash[externalInsightsTable.Rows[i]["InsightsTitle"].ToString()] += Convert.ToInt64(externalInsightsTable.Rows[i]["HitCount"]);
            }
            else
            {
                allhash[externalInsightsTable.Rows[i]["InsightsTitle"].ToString()] = Convert.ToInt64(externalInsightsTable.Rows[i]["HitCount"]);
            }
        }
    }

    var dicSort = from objDic in allhash orderby objDic.Value descending select objDic;
    string markdown = @"<markdown>";


    markdown += $@"
    | Insights Title | Expanded Count|
    | :---: | :---:|
    ";

    int count = 0;
    foreach (KeyValuePair<string, long> kvp in dicSort)
    {
        if (count++ >= 5)
            break;
        markdown += $@"| `{kvp.Key}` | {kvp.Value}|
        ";
    }
    markdown += "</markdown>";
    return markdown;
}

private static long GetInsightsExpandedTimes(string dataSource, DataTable externalInsightsTable, DataTable internalInsightsTable)
{
    long expandedCount = 0;
    if (dataSource != "2")
    {
        for (int i = 0; i < internalInsightsTable.Rows.Count; i++)
        {
            expandedCount += Convert.ToInt64(internalInsightsTable.Rows[i]["HitCount"]);
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < externalInsightsTable.Rows.Count; i++)
        {
            expandedCount += Convert.ToInt64(externalInsightsTable.Rows[i]["HitCount"]);
        }
    }

    return expandedCount;
}


private static string GetChildDetectorsQuery(string detectorId, string timeRange)
{
    return
    $@" customEvents 
    | where name contains 'ChildDetectorsSummary'
    | where timestamp >= ago({timeRange}) 
    | where customDimensions.DetectorId contains '{detectorId}'
    | extend s1 = split(operation_Name, '/')
    | extend list = split(todynamic(customDimensions.ChildDetectorsList), '}}'), site = tostring(s1[8]),detectorId=tostring(customDimensions.DetectorId)
    | extend childDetectorCount = arraylength(list), childLists = todynamic(customDimensions.ChildDetectorsList)
    | sort by session_Id asc, site asc, detectorId asc, timestamp desc 
    | summarize arg_max(childDetectorCount, *) by session_Id, site, detectorId 
    | project session_Id, timestamp, site , detectorId , childDetectorCount, list, childLists
    ";
}

private static string GetAllChildDetectorsQuery(string dataSource, DataTable internalChildDetectors, DataTable externalChildDetectors)
{
    Dictionary<string, List<string>> allhash = new Dictionary<string, List<string>>();
    Dictionary<string, string> healthStatus = new Dictionary<string, string>() {{"0", "Critical"}, {"1", "Warning"}, {"2", "Info"}, {"3", "Success"}, {"4", "None"}};
        
    
    if (dataSource != "2") {
        for (int i = 0; i < internalChildDetectors.Rows.Count; i++)
        {
            var lists1 = internalChildDetectors.Rows[i]["childLists"].ToString().Split(new string[] {"[{", "},{",  "}]"}, StringSplitOptions.RemoveEmptyEntries);
            //   res.AddInsight(InsightStatus.Success, internalChildDetectors.Rows[i]["childLists"].ToString());
            //  res.AddInsight(InsightStatus.Success, lists1.Length.ToString());
            List<string> detectorInfo = new List<string>();

            foreach (var detectorItem in lists1)
            {
                detectorInfo = new List<string>();
            //     res.AddInsight(InsightStatus.Warning, detectorItem.ToString());
                var info = detectorItem.Split(new string[] {","}, StringSplitOptions.RemoveEmptyEntries);
                var childDetectorName = "";
                for (int k = 0; k < info.Length; k++)
                {

                    //  "ChildDetectorName":"Check Swap Operations","ChildDetectorId":"swap","ChildDetectorStatus":0,"ChildDetectorLoadingStatus":1
                    var pair = info[k].Split(new string[] {":"}, StringSplitOptions.RemoveEmptyEntries);
                    if (pair.Length > 1) {
                        if (k == 0) {
                            childDetectorName = pair[1];
                            if (allhash.ContainsKey(childDetectorName)) {
                                // Count++;
                                allhash[childDetectorName][3] = (Convert.ToInt64(allhash[childDetectorName][3])+1).ToString();
                                break;
                            }
                        }
                        else {
                            if (k == 2) {
                                detectorInfo.Add(healthStatus[pair[1]]);
                            }
                            else {
                                detectorInfo.Add(pair[1]);
                            }

                            
                        }

                            if ( k == lists1.Length-1) {
                                detectorInfo.Add("1");
                                allhash[childDetectorName] = detectorInfo;
                            }
                    }
                }
            }
        }     
    }

    
    if (dataSource != "1") {
        for (int i = 0; i < externalChildDetectors.Rows.Count; i++)
        {
            var lists1 = externalChildDetectors.Rows[i]["childLists"].ToString().Split(new string[] {"[{", "},{",  "}]"}, StringSplitOptions.RemoveEmptyEntries);
            //   res.AddInsight(InsightStatus.Success, externalChildDetectors.Rows[i]["childLists"].ToString());
            //  res.AddInsight(InsightStatus.Success, lists1.Length.ToString());
            List<string> detectorInfo = new List<string>();

            foreach (var detectorItem in lists1)
            {
                detectorInfo = new List<string>();
            //     res.AddInsight(InsightStatus.Warning, detectorItem.ToString());
                var info = detectorItem.Split(new string[] {","}, StringSplitOptions.RemoveEmptyEntries);
                var childDetectorName = "";
                for (int k = 0; k < info.Length; k++)
                {

                    //  "ChildDetectorName":"Check Swap Operations","ChildDetectorId":"swap","ChildDetectorStatus":0,"ChildDetectorLoadingStatus":1
                    var pair = info[k].Split(new string[] {":"}, StringSplitOptions.RemoveEmptyEntries);
                    if (pair.Length > 1) {
                        if (k == 0) {
                            childDetectorName = pair[1];
                            if (allhash.ContainsKey(childDetectorName)) {
                                allhash[childDetectorName][3] = (Convert.ToInt64(allhash[childDetectorName][3])+1).ToString();
                                break;
                            }
                        }
                        else {
                            // Mapping status enum to string
                            if (k == 2) {
                                detectorInfo.Add(healthStatus[pair[1]]);
                            }
                            else {
                                detectorInfo.Add(pair[1]);
                            } 
                        }

                        if ( k == lists1.Length-1) {
                            detectorInfo.Add("1");
                            allhash[childDetectorName] = detectorInfo;
                        }
                    }
                }
            }
        }     
    }
    
    var dicSort = from objDic in allhash orderby objDic.Key ascending select objDic;

    if (allhash.Count == 0)
        return "Not Available";
    string markdown = @"<markdown>";

    markdown += $@"
    | Child detector | Status | Count |
    | :---: | :---:| :---:|
    ";

    foreach (KeyValuePair<string, List<string>> kvp in dicSort)
    {

        markdown += $@"| `{kvp.Key}` | ` {kvp.Value[1]}` |`{kvp.Value[3]} `|
        ";
    }
    markdown += "</markdown>";
    return markdown;
}

[SystemFilter]
[Definition(Id = "__analytics", Name = "Business analytics", Author = "xipeng,shgup", Description = "")]
public async static Task<Response> Run(DataProviders dp, Dictionary<string, dynamic> cxt, Response res)
{
    string detectorId = cxt["detectorId"].ToString();
    string dataSource = cxt["dataSource"].ToString();
    string timeRange = cxt["timeRange"].ToString() + "h";
    string timeGrain = "5m";

    if (timeRange == "72h")
        timeGrain = "15m";
    else if (timeRange == "168h")
        timeGrain = "1h";

    var uniqueSubscription = await dp.Kusto.ExecuteClusterQuery(GetUniqueSubscriptionQuery(detectorId, dataSource, timeRange));
    var uniqueResource = await dp.Kusto.ExecuteClusterQuery(GetUniqueResourceQuery(detectorId, dataSource, timeRange));
    long expandedTimes = 0;


    string deflectionCount = "0";
    string deflectionMonth = "";


    SupportTopic[] supportTopicList = null;
    if (cxt.ContainsKey("supportTopicList"))
    {
           supportTopicList  = cxt["supportTopicList"];
    }

    if(supportTopicList == null || supportTopicList.Length == 0)
    {
        deflectionCount = "N/A";
    }
    else
    {
        List<Task<DataTable>> deflectionTasks = new List<Task<DataTable>>();
        foreach(var topic in supportTopicList)
        {
            if (supportTopicMap.ContainsKey(topic.Id))
            {
                deflectionTasks.Add(dp.Kusto.ExecuteClusterQuery(GetTotalDeflectionQuery(supportTopicMap[topic.Id].Item1, supportTopicMap[topic.Id].Item2)));
            }
        }


        var deflectionTableList = await Task.WhenAll(deflectionTasks);

        if (deflectionTableList != null && deflectionTableList.Length > 0)
        {
            double totalNumerator = 0;
            double totalDenominator = 0;
            double deflectionPercentage = 0;
            DateTime timePeriod = DateTime.UtcNow.AddMonths(-1);
            foreach(var table in deflectionTableList)
            {
                if(table != null && table.Rows != null && table.Rows.Count > 0)
                {
                    totalNumerator += Convert.ToDouble(table.Rows[0]["Numerator"].ToString());
                    totalDenominator += Convert.ToDouble(table.Rows[0]["Denominator"].ToString());
                    timePeriod = DateTime.Parse(table.Rows[0]["period"].ToString());
                }
            }

            if(totalDenominator != 0)
            {
                deflectionPercentage = Math.Round(100.0 * totalNumerator / totalDenominator, 1);

                deflectionCount = $"{deflectionPercentage} % ({Convert.ToInt64(totalNumerator)}/{Convert.ToInt64(totalDenominator)})";
                deflectionMonth = $"(Month : {timePeriod.ToString("MM/yy")})";
            }
        }
    }

    var ds1 = new DataSummary($"Case Deflection {deflectionMonth}", $"{deflectionCount}", "yellowgreen");
    var ds2 = new DataSummary("Unique Subs", "0", "blue");
    var ds3 = new DataSummary("Unique Resources", "0", "yellow");
    var ds5 = new DataSummary("Critical Insights", "0", "orangered");
    // limegreen orangered yellow
    if (uniqueSubscription.Rows.Count > 0)
    {
        ds2 = new DataSummary("Unique Subs", uniqueSubscription.Rows[0][0].ToString(), "dodgerblue");
    }

    if (uniqueResource.Rows.Count > 0)
    {
        ds3 = new DataSummary("Unique Resources", uniqueResource.Rows[0][0].ToString(), "hotpink");
    }

    // AppInsights Table
    await dp.AppInsights.SetAppInsightsKey("73bff7df-297f-461e-8c14-377774ae7c12", "vkd6p42lgxcpeh04dzrwayp8zhhrfoeaxtcagube");
    var internalInsightsTable = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsQuery(detectorId, timeRange));
    var internalChildDetectors = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsQuery(detectorId, timeRange));
 
    await dp.AppInsights.SetAppInsightsKey("bda43898-4456-4046-9a7c-9ffa83f47c33", "2ejbz8ipv8uzgq14cjyqsimvh0hyjoxjcr7mpima");
    var externalInsightsTable = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsQuery(detectorId, timeRange));
    var externalChildDetectors = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsQuery(detectorId, timeRange));

    expandedTimes = GetInsightsExpandedTimes(dataSource, externalInsightsTable, internalInsightsTable);
    var ds4 = new DataSummary("Insights Expanded", expandedTimes.ToString(), "mediumpurple");
    res.AddDataSummary(new List<DataSummary>() { ds1, ds2, ds3, ds4 });

    // Unique subscriptions and resources graph
    var usersandResourcesRangeTable = new DiagnosticData()
    {
        Table = await dp.Kusto.ExecuteClusterQuery(GetUserandResourceRangeQuery(detectorId, dataSource, timeRange, timeGrain)),
        //   RenderingProperties = new Rendering(RenderingType.Table)
        RenderingProperties = new TimeSeriesRendering()
        {
            Title = "Unique Subscriptions and Resources",
            GraphType = TimeSeriesType.BarGraph,
            GraphOptions = new
            {
                color = new string[] { "dodgerblue", "hotpink", "#107E7D", "#8A2BE2", "#D2691E", "#008B8B", "#4298f4", "rgb(55, 175, 49)" },
                forceY = new int[] { 0, 5 },
                yAxis = new
                {
                    axisLabel = "Unique Users/Resources Count"
                }
            }
        }
    };

    res.Dataset.Add(usersandResourcesRangeTable);

    Dictionary<string, string> insightsBody = new Dictionary<string, string>();
    string markdownstr = GetAllCustomEventsQuery(dataSource, externalInsightsTable, internalInsightsTable);
    insightsBody.Add("Insights Ranking", markdownstr);
    Insight allInsight = new Insight(InsightStatus.Success, "💖 Top 5 expanded Insights", insightsBody, true);
    res.AddInsight(allInsight);
    markdownstr = "";


    Dictionary<string, string> childDetectorsBody = new Dictionary<string, string>();
    markdownstr = GetAllChildDetectorsQuery(dataSource, internalChildDetectors, externalChildDetectors);
    childDetectorsBody.Add("Children detectors count (Per Session/Site)", markdownstr);
    Insight allDetectors = new Insight(InsightStatus.Success, "✨ Children detectors ", childDetectorsBody, true);

    if (internalChildDetectors.Rows.Count > 0 || externalChildDetectors.Rows.Count > 0) {
        res.AddInsight(allDetectors);
    }

    res.AddInsight(InsightStatus.Success, "⭐ Detector Rating coming soon");

    return res;
}

#region Deflection Metrics

private static string GetTotalDeflectionQuery(string category, string supportTopic)
{
    return $@"
    cluster('usage360').database('Product360').
    SupportProductionDeflectionMonthlyVer1023
    | extend period = Timestamp
    | where period >= ago(60d)
    | where SupportTopicL2 =~ '{category}'
    | where SupportTopicL3 =~ '{supportTopic}'
    | where(DerivedProductIDStr == @'14748')
    | where DenominatorQuantity != 0 
    | summarize qty = sum(NumeratorQuantity) / sum(DenominatorQuantity),Numerator = sum(NumeratorQuantity), Denominator = sum(DenominatorQuantity) by period
    | top 1 by period desc
    ";
}

// TODO : This is a Hack right now and we should figure out a way to programatically get this.
private static Dictionary<string, Tuple<string, string>> supportTopicMap = new Dictionary<string, Tuple<string, string>>()
{
    {"32542218", new Tuple<string, string>("Availability, Performance, and Application Issues", "Web app down or reporting errors")},
    {"32583701", new Tuple<string, string>("Availability, Performance, and Application Issues", "Web app experiencing high CPU")},
    {"32570954", new Tuple<string, string>("Availability, Performance, and Application Issues", "Web app restarted")},
    {"32457411", new Tuple<string, string>("Availability, Performance, and Application Issues", "Web app slow")},
    {"32581616", new Tuple<string, string>("Availability, Performance, and Application Issues", "Web app experiencing high memory usage")},
    
    {"32542210", new Tuple<string, string>("Configuration and Management", "IP configuration")},
    {"32542208", new Tuple<string, string>("Configuration and Management", "Backup and Restore")},
    {"32440122", new Tuple<string, string>("Configuration and Management", "Configuring custom domain names")},
    {"32440123", new Tuple<string, string>("Configuration and Management", "Configuring SSL")},
    {"32581615", new Tuple<string, string>("Configuration and Management", "Deployment slots (create, swap, and so on)")},
    {"32581619", new Tuple<string, string>("Configuration and Management", "Moving resources")},
    {"32542211", new Tuple<string, string>("Configuration and Management", "Scaling")},
    {"32581628", new Tuple<string, string>("Deployment", "ARM template")},
    {"32542213", new Tuple<string, string>("Deployment", "FTP")},
    {"32542214", new Tuple<string, string>("Deployment", "Git, GitHub, BitBucket, Dropbox")},
    {"32588774", new Tuple<string, string>("Deployment", "Visual Studio")},
    {"32542215", new Tuple<string, string>("Deployment", "Other")},

    {"32589281", new Tuple<string, string>("How Do I", "IP configuration")},
    {"32589276", new Tuple<string, string>("How Do I", "Configure backup and restore")},
    {"32589277", new Tuple<string, string>("How Do I", "Configure domains and certificates")},

    // ASE
    {"32608422", new Tuple<string, string>("Networking", "Configuring force tunneling")},
    {"32608423", new Tuple<string, string>("Networking", "Configuring NSGs")},
    {"32608425", new Tuple<string, string>("Networking", "Configuring UDRs")},
    {"32608427", new Tuple<string, string>("Networking", "Connectivity (VNet or on-prem)")}
};


#endregion