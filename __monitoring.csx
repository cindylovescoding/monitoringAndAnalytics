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

private static string GetDataSummaryQuery(string detectorId, string dataSource, string timeRange)
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '2002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
    | summarize totalReqs = count(), failedReqs = countif(StatusCode>=500), avgLatency = avg(LatencyInMilliseconds), p90Latency = percentiles(LatencyInMilliseconds, 90)
    | extend availability = 100.0 - (failedReqs*100.0/totalReqs)
    ";
}

private static string GetRequestStatusQuery(string detectorId, string dataSource, string timeRange, string timeGrain="5m")
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '2002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
    | project PreciseTimeStamp, Http2xx = (StatusCode / 100 == 2), Http3xx = (StatusCode / 100 == 3), Http4xx = (StatusCode / 100 == 4), Http5xx = (StatusCode / 100 == 5)
    | summarize TotalRequests = count(), count(Http2xx), count(Http3xx), count(Http4xx), count(Http5xx)  by bin(PreciseTimeStamp, {timeGrain})
    | order by PreciseTimeStamp asc
    ";
}


private static string GetResponseTimeQuery(string detectorId, string dataSource, string timeRange, string timeGrain = "5m")
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '2002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
    |summarize percentiles(LatencyInMilliseconds, 50, 90, 99) by bin(PreciseTimeStamp, {timeGrain})
    ";
}

private static string GetExceptionsQuery(string detectorId, string dataSource, string timeRange)
{
    string tableName = GetTableName(dataSource, timeRange);
    return
    $@"{tableName}
    | where EventId == '2002'
    | where Address contains strcat('/detectors/', '{detectorId}') and  Address !contains strcat('/detectors/', '{detectorId}', '/statistics')
    | project RequestId, Address, StatusCode
    | join
    (
        {tableName}
        | where  EventId == '2001'
        | project PreciseTimeStamp, RequestId, ExceptionType, ExceptionDetails 
    )
    on RequestId 
    | project PreciseTimeStamp, StatusCode, Address, ExceptionDetails
    ";
}

[SystemFilter]
[Definition(Id = "__monitoring", Name = "📈 Monitoring Report", Author = "xipeng,shgup", Description = "")]
public async static Task<Response> Run(DataProviders dp, Dictionary<string, dynamic> cxt, Response res)
{
    string detectorId = cxt["detectorId"].ToString();
    string dataSource = cxt["dataSource"].ToString();
    string timeRange = cxt["timeRange"].ToString()+"h";
    string timeGrain = "5m";

    // mediumseagreen yellowgreen dodgerblue hotpink mediumpurple orangered limegreen yellow
    DataSummary ds1 = new DataSummary("Availability", "100", "mediumseagreen");
    DataSummary ds2 = new DataSummary("TotalRequests", "0", "hotpink");
    DataSummary ds3 = new DataSummary("FailedRequests", "0", "orangered");
    DataSummary ds4 = new DataSummary("Avg. Latency (ms)", "0", "mediumpurple");
    DataSummary ds5 = new DataSummary("P_90 Latency (ms)", "0", "dodgerblue");

    if (timeRange == "72h")
        timeGrain = "15m";
    else if (timeRange == "168h")
        timeGrain = "1h";
    var availabilityTable = await dp.Kusto.ExecuteClusterQuery(GetDataSummaryQuery(detectorId, dataSource, timeRange));
    if (availabilityTable.Rows.Count > 0)
    {
        ds1 = new DataSummary("Availability", decimal.Parse(availabilityTable.Rows[0]["availability"].ToString()).ToString("0.00")+"%", "mediumseagreen");
        ds2 = new DataSummary("TotalRequests", availabilityTable.Rows[0]["totalReqs"].ToString(), "hotpink");
        ds3 = new DataSummary("FailedRequests", availabilityTable.Rows[0]["failedReqs"].ToString(), "orangered");
        ds4 = new DataSummary("Avg. Latency (ms)", decimal.Parse(availabilityTable.Rows[0]["avgLatency"].ToString()).ToString("0.00"), "mediumpurple");
        ds5 = new DataSummary("P_90 Latency (ms)", decimal.Parse(availabilityTable.Rows[0]["p90Latency"].ToString()).ToString("0.00"), "dodgerblue");
    }

    res.AddDataSummary(new List<DataSummary>() { ds1, ds2, ds3, ds4, ds5 });

    // Request Status time series
    var httpstatuscodes = new DiagnosticData()
    {
        Table = await dp.Kusto.ExecuteClusterQuery(GetRequestStatusQuery(detectorId, dataSource, timeRange, timeGrain)),
      //  RenderingProperties = new Rendering(RenderingType.Table)
        RenderingProperties = new TimeSeriesRendering(){
            Title = "Trends by Status Code",
            GraphType = TimeSeriesType.LineGraph,
            GraphOptions = new {
                color = new string[]{"dodgerblue", "mediumseagreen", "mediumpurple","hotpink", "red"},
                forceY = new int[] { 0, 2 }, //This means y axis will be 0 to 100.
                yAxis = new {
                    axisLabel = "Requests Count"
                } 
            }
        }
    };
    
    res.Dataset.Add(httpstatuscodes);
    
    // Response time time series
    var responseTimeSeries = new DiagnosticData()
    {
        Table = await dp.Kusto.ExecuteClusterQuery(GetResponseTimeQuery(detectorId, dataSource, timeRange, timeGrain)),
     //   RenderingProperties = new Rendering(RenderingType.Table)
        RenderingProperties = new TimeSeriesRendering(){
            Title = "Response time",
            GraphType = TimeSeriesType.LineGraph,
            GraphOptions = new {
                color = new string[]{"dodgerblue", "mediumpurple","hotpink"},
                forceY = new int[] { 0, 10000 }, //This means y axis will be 0 to 100.
                yAxis = new {
                    axisLabel = "Response time"
                } 
            }
        }
    };

    // Request Exceptions
    res.Dataset.Add(responseTimeSeries);

    var requestExceptions = new DiagnosticData()
    {
        Table = await dp.Kusto.ExecuteClusterQuery(GetExceptionsQuery(detectorId, dataSource, timeRange)),
        RenderingProperties = new Rendering(RenderingType.Table){
            Title = "Exceptions"
        }
    };

    res.Dataset.Add(requestExceptions);


    return res;
}