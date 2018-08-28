 using System.Linq;
using Diagnostics.ModelsAndUtils.Models;
using System.Reflection;


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

private static string GetChildDetectorsExpandedQuery1(string detectorId, string timeRange)
{
    return
    $@"   customEvents 
    | where timestamp >= ago({timeRange})
    | where name contains 'ChildDetectorClicked' 
    | where customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.IsExpanded == 'true'
    | extend s1 = split(operation_Name, '/')
    | extend site = tostring(s1[8]),detectorId=tostring(customDimensions.DetectorId)
    | extend ChildDetectorId = tostring(customDimensions.ChildDetectorId) 
    | summarize HitCount = count() by ChildDetectorId 
    ";
}

private static string GetChildDetectorsExpandedQuery(string detectorId, string timeRange)
{
    return 
    $@" 
customEvents 
    | where name contains 'ChildDetectorsSummary'
    | where timestamp >= ago({timeRange}) and timestamp >= datetime(2018-08-22T00:59:08.564Z)
    | where customDimensions.DetectorId contains '{detectorId}'
    | extend ChildDetectorInfo = todynamic(tostring(customDimensions.ChildDetectorsList) ) 
    | mvexpand ChildDetectorInfo
    | extend childDetectorName = tostring(ChildDetectorInfo['ChildDetectorName']), childDetectorStatus = tostring(ChildDetectorInfo['ChildDetectorStatus']), loadingStatus = tolong(ChildDetectorInfo['ChildDetectorLoadingStatus']), childDetectorId = tostring(ChildDetectorInfo['ChildDetectorId']) 
    | project ChildDetectorInfo, childDetectorName , childDetectorId, childDetectorStatus, loadingStatus
    | summarize showedCount = tostring(count()) by childDetectorName, childDetectorId, childDetectorStatus
| join kind= leftouter (
customEvents 
// datetime(2015-12-31 23:59:59.9)
    | where timestamp >= ago({timeRange}) and timestamp >= datetime(2018-08-22T00:59:08.564Z)
    | where name contains 'ChildDetectorClicked' 
    | where customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.IsExpanded == 'true'
    | where isnotempty(customDimensions['Status'])
    | extend  childDetectorName = tostring(customDimensions['ChildDetectorName']),  childDetectorId = tostring(customDimensions['ChildDetectorId']), childDetectorStatus = tostring(customDimensions['Status'])
    | summarize hitCount = tostring(count()) by  childDetectorName, childDetectorId, childDetectorStatus
)
on childDetectorName, childDetectorId, childDetectorStatus
| project-away childDetectorName1, childDetectorStatus1, childDetectorId1
| sort by hitCount 
    ";
}

private static string GetInsightsShowingAndClickQuery(string detectorId, string timeRange)
{
    // to filter insights click with status : timestamp > todatetime('2018-08-22T19:24:48.521Z')

    // Second join     | where isempty(tostring(customDimensions.Status)) == false
    return
    $@"
    customEvents 
    | where timestamp > ago({timeRange})
    | where name contains 'InsightsSummary' and customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.DetectorId !contains '__analytics' and customDimensions.DetectorId !contains '__monitoring'
    | project timestamp, customDimensions = todynamic(tostring(customDimensions)), insightsSummary =  todynamic(tostring(customDimensions.InsightsSummary)),  InsightsList =  todynamic(tostring(customDimensions.InsightsList))
    | mvexpand InsightsList
    | extend InsightTitle = tostring(InsightsList.Name), InsightStatus = tostring(InsightsList.Status)
    | summarize ShowedCount = count() by InsightTitle, InsightStatus
| join kind=leftouter(
customEvents 
    | where timestamp >= ago({timeRange})
    | where name contains 'InsightsTitleClicked' 
    | where customDimensions.DetectorId !contains '__analytics' and customDimensions.DetectorId !contains '__monitoring'
    | where customDimensions.DetectorId contains '{detectorId}'
    | where customDimensions.IsExpanded == 'true' 
    | project InsightTitle = tostring(customDimensions.Title), InsightStatus = tostring(customDimensions.Status) 
    | summarize HitCount = count() by InsightTitle, InsightStatus
    | extend statusMapping=dynamic(['Critical', 'Warning', 'Info', 'Success', 'None']) 
    | extend InsightStatus = tostring(statusMapping[toint(InsightStatus)])
    | project InsightTitle, InsightStatus ,  HitCount
) on InsightTitle, InsightStatus 
| project-away InsightTitle1, InsightStatus1
 | sort by HitCount desc  
    ";
}


private static string GetInsightsExpandedMapping(string dataSource, DataTable internalTable, DataTable externalTable, Dictionary<string, long> topInsights)
{
    // table schema: InsightTitle, InsightStatus, ShowedCount, HitCount
    // Dictionary value list will be: 0: InsightTitle, 1: InsightStatus, 2: ShowedCount, 3, HitCount
    Dictionary<string, List<string>> allhash = new Dictionary<string, List<string>>();
 //   string[] status = new string[5]{"Critical", "Warning", "Info", "Success", "None"};
    Dictionary<string, int> statusMapping = new Dictionary<string, int> {
        {"Critical", 0},
        {"Warning", 1},
        {"Info", 2},
        {"Success", 3},
        {"None", 4}
    };
    if (dataSource != "2")
    {
        for (int i = 0; i < internalTable.Rows.Count; i++)
        {
            string hashkey = internalTable.Rows[i]["InsightTitle"].ToString() + internalTable.Rows[i]["InsightStatus"].ToString();
            string hitCount = String.IsNullOrEmpty(internalTable.Rows[i]["HitCount"].ToString()) ?  "0": internalTable.Rows[i]["HitCount"].ToString();
            if (allhash.ContainsKey(hashkey))
            {
                allhash[hashkey][2] = Convert.ToString(Convert.ToInt64(allhash[hashkey][2]) + Convert.ToInt64(internalTable.Rows[i]["ShowedCount"]));
                allhash[hashkey][3] = Convert.ToString(Convert.ToInt64(allhash[hashkey][3]) + Convert.ToInt64(hitCount));
            }
            else
            {
                int statusIndex = statusMapping[internalTable.Rows[i]["InsightStatus"].ToString()];
                 allhash[hashkey] = new List<string> {internalTable.Rows[i]["InsightTitle"].ToString(), EventLogIcons.statusIcon[statusIndex] , internalTable.Rows[i]["showedCount"].ToString(), hitCount.ToString()};
               //    allhash[hashkey] = new List<string> { status[statusIndex], internalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), "", ""};
            }
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < externalTable.Rows.Count; i++)
        {
            string hashkey = externalTable.Rows[i]["InsightTitle"].ToString() + externalTable.Rows[i]["InsightStatus"].ToString();
            string hitCount = String.IsNullOrEmpty(externalTable.Rows[i]["HitCount"].ToString()) ?  "0": externalTable.Rows[i]["HitCount"].ToString();
            if (allhash.ContainsKey(hashkey))
            {
                allhash[hashkey][2] = Convert.ToString(Convert.ToInt64(allhash[hashkey][2]) + Convert.ToInt64(externalTable.Rows[i]["ShowedCount"]));
                allhash[hashkey][3] = Convert.ToString(Convert.ToInt64(allhash[hashkey][3]) + Convert.ToInt64(hitCount));
            }
            else
            {
                int statusIndex = statusMapping[externalTable.Rows[i]["InsightStatus"].ToString()];
                 allhash[hashkey] = new List<string> {externalTable.Rows[i]["InsightTitle"].ToString(), EventLogIcons.statusIcon[statusIndex] , externalTable.Rows[i]["showedCount"].ToString(), hitCount.ToString()};
               //    allhash[hashkey] = new List<string> { status[statusIndex], internalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), "", ""};
            }
        }
    }
        // table schema: childDetectorName, childDetectorId, childDetectorStatus, showedCount, hitCount
        // Dictionary value list will be: 0: childDetectorName, 1: childDetectorStatus, 2: showedCount, 3, hitCount

    if (allhash.Count == 0) {
        return "";
    }

    foreach (KeyValuePair<string, List<string>> kvp in allhash)
    {
        if (topInsights.ContainsKey(kvp.Value[0])){
            allhash[kvp.Key][3] = topInsights[kvp.Value[0]].ToString();
        }
    }

    var dicSort = from objDic in allhash orderby objDic.Value[3] descending select objDic;
    string markdown = @"<markdown>";

    markdown += $@"
    | Insight Name | Status | Showed Count | Expanded Count |
    | :---: | :---:| :---:| :---:|
    ";
     
    foreach (KeyValuePair<string, List<string>> kvp in dicSort)
    {
            // You will need an "Enter" here to ensure several lines.
        markdown += $@"| `{kvp.Value[0]}` | {kvp.Value[1]} | {kvp.Value[2]}| {kvp.Value[3]} |
        ";
    }

    markdown += "</markdown>";

    return markdown;
}

private static class EventLogIcons
{
    public static string Critical = @"<i class=""fa fa-times-circle"" style=""color:#ce4242"" aria-hidden=""true""></i>";

    public static string Error = @"<i class=""fa fa-exclamation-circle"" style=""color:red"" aria-hidden=""true""></i>";

    public static string Warning = @"<i class=""fa fa-exclamation-triangle"" style=""color:#ff9104"" aria-hidden=""true""></i>";

    public static string Info = @"<i class=""fa fa-info-circle"" style=""color:#3a9bc7"" aria-hidden=""true""></i>";

    public static string Verbose = @"<i class=""fa fa-exclamation-circle"" style=""color:#a9abad"" aria-hidden=""true""></i>";

    public static string Success = @"<i class=""fa fa-check-circle"" style=""color:#3da907"" aria-hidden=""true""></i>";

    public static string[] statusIcon = new string[] {Critical, Warning, Info, Success, Verbose};
} 

private static string GetChildDetectorsExpandedMapping(string dataSource, DataTable internalChildDetectorsTable, DataTable externalChildDetectorsTable)
{
    // table schema: childDetectorName, childDetectorId, childDetectorStatus, showedCount, hitCount
    // Dictionary value list will be: 0: childDetectorName, 1: childDetectorStatus, 2: showedCount, 3, hitCount
    Dictionary<string, List<string>> allhash = new Dictionary<string, List<string>>();
    string[] status = new string[5]{"Critical", "Warning", "Info", "Success", "None"};
    if (dataSource != "2")
    {
        for (int i = 0; i < internalChildDetectorsTable.Rows.Count; i++)
        {
            string hashkey = internalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString() + internalChildDetectorsTable.Rows[i]["childDetectorStatus"].ToString();
            string hitCount = String.IsNullOrEmpty(internalChildDetectorsTable.Rows[i]["hitCount"].ToString()) ?  "0": internalChildDetectorsTable.Rows[i]["hitCount"].ToString();
            if (allhash.ContainsKey(hashkey))
            {
                allhash[hashkey][2] = Convert.ToString(Convert.ToInt64(allhash[hashkey][2]) + Convert.ToInt64(internalChildDetectorsTable.Rows[i]["showedCount"]));
                allhash[hashkey][3] = Convert.ToString(Convert.ToInt64(allhash[hashkey][3]) + Convert.ToInt64(hitCount));
            }
            else
            {
                var statusIndex =  Convert.ToInt32(internalChildDetectorsTable.Rows[i]["childDetectorStatus"]);
                 allhash[hashkey] = new List<string> {internalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), EventLogIcons.statusIcon[statusIndex], internalChildDetectorsTable.Rows[i]["showedCount"].ToString(), hitCount.ToString()};
             //       allhash[hashkey] = new List<string> {internalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), status[statusIndex], internalChildDetectorsTable.Rows[i]["showedCount"].ToString(), hitCount.ToString()};
               //    allhash[hashkey] = new List<string> { status[statusIndex], internalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), "", ""};
            }
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < externalChildDetectorsTable.Rows.Count; i++)
        {
            string hashkey = externalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString() + externalChildDetectorsTable.Rows[i]["childDetectorStatus"].ToString();
            if (allhash.ContainsKey(hashkey))
            {
                allhash[hashkey][2] = Convert.ToString(Convert.ToInt64(allhash[hashkey][2]) + Convert.ToInt64(externalChildDetectorsTable.Rows[i]["showedCount"]));
                allhash[hashkey][3] = Convert.ToString(Convert.ToInt64(allhash[hashkey][3]) + Convert.ToInt64(externalChildDetectorsTable.Rows[i]["hitCount"]));
            }
            else
            {
                allhash[hashkey] = new List<string> {externalChildDetectorsTable.Rows[i]["childDetectorName"].ToString(), EventLogIcons.statusIcon[Convert.ToInt32(externalChildDetectorsTable.Rows[i]["childDetectorStatus"])], externalChildDetectorsTable.Rows[i]["showedCount"].ToString(), externalChildDetectorsTable.Rows[i]["hitCount"].ToString()};
            }
        }
    }

        // table schema: childDetectorName, childDetectorId, childDetectorStatus, showedCount, hitCount
        // Dictionary value list will be: 0: childDetectorName, 1: childDetectorStatus, 2: showedCount, 3, hitCount

    var dicSort = from objDic in allhash orderby objDic.Value[3] descending select objDic;
    string markdown = @"<markdown>";

    markdown += $@"
    | Child Detector Name | Status | Showed Count | Expanded Count |
    | :---: | :---:| :---:| :---:|
    ";
        
    foreach (KeyValuePair<string, List<string>> kvp in dicSort)
    {
            // You will need an "Enter" here to ensure several lines.
        markdown += $@"| `{kvp.Value[0]}` | {kvp.Value[1]} | {kvp.Value[2]}| {kvp.Value[3]} |
        ";
    }

    markdown += "</markdown>";

    return markdown;
}


private static Dictionary<string, long> GetChildDetectorsExpandedMapping1(string dataSource, DataTable internalChildDetectorsTable, DataTable externalChildDetectorsTable)
{
    Dictionary<string, long> allhash = new Dictionary<string, long>();

    if (dataSource != "2")
    {
        for (int i = 0; i < internalChildDetectorsTable.Rows.Count; i++)
        {
            if (allhash.ContainsKey(internalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()))
            {
                allhash[internalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()] += Convert.ToInt64(internalChildDetectorsTable.Rows[i]["HitCount"]);
            }
            else
            {
                allhash[internalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()] = Convert.ToInt64(internalChildDetectorsTable.Rows[i]["HitCount"]);
            }
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < externalChildDetectorsTable.Rows.Count; i++)
        {
            if (allhash.ContainsKey(externalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()))
            {
                allhash[externalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()] += Convert.ToInt64(externalChildDetectorsTable.Rows[i]["HitCount"]);
            }
            else
            {
                allhash[externalChildDetectorsTable.Rows[i]["ChildDetectorId"].ToString()] = Convert.ToInt64(externalChildDetectorsTable.Rows[i]["HitCount"]);
            }
        }
    }

    return allhash;
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

private static string GetAllInsightsCount(string detectorId, string timeRange)
{
    return
    $@" customEvents 
    | where timestamp >= ago({timeRange}) 
    | where name contains 'InsightsSummary' and customDimensions.DetectorId contains '{detectorId}'
    | project timestamp, customDimensions = todynamic(tostring(customDimensions)), insightsSummary =  todynamic(tostring(customDimensions.InsightsSummary)),  insightsList =  todynamic(tostring(customDimensions.InsightsList))
    | extend TotalCount = tolong(insightsSummary.Total), CriticalCount = tolong(insightsSummary.Critical), WarningCount = tolong(insightsSummary.Warning), SuccessCount = tolong(insightsSummary.Success), InfoCount = tolong(insightsSummary.Info),  DefaultCount = tolong(insightsSummary.Default)
    | summarize TotalCount = sum(TotalCount),  CriticalCount = sum(CriticalCount),  WarningCount = sum(WarningCount),  SuccessCount = sum(SuccessCount), InfoCount = sum(InfoCount), DefaultCount = sum(DefaultCount)
    ";
}

private static string GetTotalInsightsMarkdown (string dataSource, DataTable internalAllInsightsCount, DataTable externalAllInsightsCount, out string criticalInsightsCount)
{
    Dictionary<string, long> allhash = new Dictionary<string, long>();
    string[] insightStatus = new string[6]{"TotalCount", "CriticalCount", "WarningCount", "SuccessCount", "InfoCount", "DefaultCount"};
    long[] insightStatusCount = new long[6];
    if (dataSource != "2")
    {
        for (int i = 0; i < insightStatusCount.Length; i++)
        {
            insightStatusCount[i] += Convert.ToInt64(internalAllInsightsCount.Rows[0][insightStatus[i]]);
        }
    }

    if (dataSource != "1")
    {
        for (int i = 0; i < insightStatusCount.Length; i++)
        {
            insightStatusCount[i] += Convert.ToInt64(externalAllInsightsCount.Rows[0][insightStatus[i]]);
        }
    }

    string markdown = @"<markdown>";
    markdown += $@"
    | Total | Critical | Warning | Success | Info |  Default |
    | :---: | :---:| :---:| :---:| :---:| :---:|
    ";
        
    foreach (var statusCount in insightStatusCount)
    {
        markdown += $@"| `{statusCount.ToString()}` ";
    }

    markdown += $@"|";
    markdown += "</markdown>";

    criticalInsightsCount = insightStatusCount[1].ToString();
    return markdown;
}

private static Dictionary<string, long> GetTopInsightExpansion(string dataSource, DataTable externalInsightsTable, DataTable internalInsightsTable)
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
    return allhash;
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

//    int count = 0;
    foreach (KeyValuePair<string, long> kvp in dicSort)
    {
        // if (count++ >= 5)
        //     break;
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

private static string GetAllChildDetectorsQuery(string dataSource, DataTable internalChildDetectors, DataTable externalChildDetectors, Dictionary<string, long> childDetectorsExpandedMapping)
{
    Dictionary<string, List<string>> allhash = new Dictionary<string, List<string>>();
    Dictionary<string, string> healthStatus = new Dictionary<string, string>() {{"0", "Critical"}, {"1", "Warning"}, {"2", "Info"}, {"3", "Success"}, {"4", "None"}};
        
    
    if (dataSource != "2") {
        for (int i = 0; i < internalChildDetectors.Rows.Count; i++)
        {
            var lists1 = internalChildDetectors.Rows[i]["childLists"].ToString().Split(new string[] {"[{", "},{",  "}]"}, StringSplitOptions.RemoveEmptyEntries);

            // DetectorInfo: 0: ChildDetectorId, 1: ChildDetectorStatus, 2: ChildDetectorLoadingStatus, 3: Showing Count, 4: Expanded times
            List<string> detectorInfo = new List<string>();

            foreach (var detectorItem in lists1)
            {
                detectorInfo = new List<string>();

         // Info split on: "ChildDetectorName":"Check Swap Operations","ChildDetectorId":"swap","ChildDetectorStatus":0,"ChildDetectorLoadingStatus":1
                var info = detectorItem.Split(new string[] {","}, StringSplitOptions.RemoveEmptyEntries);
                var childDetectorName = "";
                var childDetectorId = "";
                for (int k = 0; k < info.Length; k++)
                {
                    var pair = info[k].Split(new string[] {":"}, StringSplitOptions.RemoveEmptyEntries);
                    if (pair.Length > 1) {

                        if (k == 0) {
                            childDetectorName = pair[1];
                            if (allhash.ContainsKey(childDetectorName)) {
                                allhash[childDetectorName][3] = (Convert.ToInt64(allhash[childDetectorName][3])+1).ToString();
                                break;
                            }
                        }
                        else  if (k == 1) {
                            childDetectorId = pair[1].Split('"', StringSplitOptions.RemoveEmptyEntries)[0];
                        }

                        if (k == 2) {
                            detectorInfo.Add(healthStatus[pair[1]]);
                        }
                        else {
                            detectorInfo.Add(pair[1]);
                        }

                        if ( k == lists1.Length-1) {
                            detectorInfo.Add("1");
                            if (childDetectorsExpandedMapping.ContainsKey(childDetectorId)) {
                                detectorInfo.Add(childDetectorsExpandedMapping[childDetectorId].ToString());
                            }
                            else {
                                detectorInfo.Add("0");
                             }
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

            List<string> detectorInfo = new List<string>();

            foreach (var detectorItem in lists1)
            {
                detectorInfo = new List<string>();
                var info = detectorItem.Split(new string[] {","}, StringSplitOptions.RemoveEmptyEntries);
                var childDetectorName = "";
                var childDetectorId = "";
                for (int k = 0; k < info.Length; k++)
                {
                    var pair = info[k].Split(new string[] {":"}, StringSplitOptions.RemoveEmptyEntries);
                    if (pair.Length > 1) {

                        if (k == 0) {
                            childDetectorName = pair[1];
                            if (allhash.ContainsKey(childDetectorName)) {
                                allhash[childDetectorName][3] = (Convert.ToInt64(allhash[childDetectorName][3])+1).ToString();
                                break;
                            }
                        }
                        else  if (k == 1) {
                            childDetectorId = pair[1].Split('"', StringSplitOptions.RemoveEmptyEntries)[0];
                        }

                        if (k == 2) {
                            detectorInfo.Add(healthStatus[pair[1]]);
                        }
                        else {
                            detectorInfo.Add(pair[1]);
                        }

                        if ( k == lists1.Length-1) {
                            detectorInfo.Add("1");
                            if (childDetectorsExpandedMapping.ContainsKey(childDetectorId)) {
                                detectorInfo.Add(childDetectorsExpandedMapping[childDetectorId].ToString());
                            }
                            else {
                                detectorInfo.Add("0");
                             }
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
    | Child detector | Status | Showing Count | Expanded Count |
    | :---: | :---:| :---:| :---:|
    ";

    foreach (KeyValuePair<string, List<string>> kvp in dicSort)
    {

        markdown += $@"| `{kvp.Key}` | ` {kvp.Value[1]}` |`{kvp.Value[3]} `|`{kvp.Value[4]} `|
        ";
    }
    markdown += "</markdown>";
    return markdown;
}

private static string GetSupportTopicMapQuery(string id, string pesId, string timeRange)
{
    // pesId: "16072", Id: "\\32598331"
    // Should not include timeRange cause we show monthly data currently:  | where Incidents_CreatedTime > ago({timeRange})
    return $@"
        cluster('usage360').database('Product360').
        AllCloudSupportIncidentDataWithP360MetadataMapping | where Incidents_CreatedTime > ago(30d)
        | where DerivedProductIDStr in ({pesId})
        | where Incidents_CurrentTopicIdFullPath contains '{id}' 
        | extend FullId = strcat(tostring(DerivedProductIDStr), '\\', tostring('{id}')), Id = tostring({id}), PesId = tostring(DerivedProductIDStr)
        | summarize by FullId, PesId, Id,TopicL2 = tostring( Incidents_SupportTopicL2Current) , SupportTopicL3 = tostring(Incidents_SupportTopicL3Current) , TopicIdFull = tostring(Incidents_CurrentTopicIdFullPath)
        | extend TopicL3 = iff(SupportTopicL3 == 'UNKNOWN', '', SupportTopicL3)
        | project FullId, PesId, Id, TopicL2, TopicL3 
    ";
//             AllCloudSupportIncidentDataWithP360MetadataMapping | where Incidents_CreatedTime > ago(30d)
// | where DerivedProductIDStr in ("16072")
// | summarize by Incidents_SupportTopicL2Current , Incidents_SupportTopicL3Current , Incidents_CurrentTopicIdFullPath
// | where Incidents_CurrentTopicIdFullPath contains "\\32598331" 
}
// private static Dictionary<string, Tuple<string, string>> GetSupportTopicMap()
// {
    

// }

private static string GetDeflectionTable(bool isSolution, string timeRange)
{
    string tableName = "";
    if (timeRange == "168h") {
        if (isSolution)
            tableName =  "SupportProductionDeflectionWeeklyPoPInsightsVer1023";
        else 
            tableName =  "SupportProductionDeflectionWeeklyVer1023";
    }
    else if (timeRange == "720h") {
        if (isSolution)
            tableName =  "SupportProductionDeflectionMonthlyPoPInsightsVer1023";
        else 
            tableName =  "SupportProductionDeflectionMonthlyVer1023";
    }
    return tableName;
}

[SystemFilter]
[Definition(Id = "__analytics", Name = "Business analytics", Author = "xipeng,shgup", Description = "")]
public async static Task<Response> Run(DataProviders dp, Dictionary<string, dynamic> cxt, Response res)
{
    string detectorId = cxt["detectorId"].ToString();
    string dataSource = cxt["dataSource"].ToString();
    string timeRange = cxt["timeRange"].ToString() + "h";
    string timeGrain = "30m";
    string deflectionTableName = "";
    string deflectionSolutionTable = "";
    bool isSolution = false;
    string deflectionTimeRange = "Month";

    if (timeRange == "72h") {
        timeGrain = "60m";
    }
    else if (timeRange == "168h"){
        timeGrain = "180m";
        deflectionTimeRange = "Week";
    }
    else if (timeRange == "720h") {
        timeGrain = "720m";
        deflectionTimeRange = "Month";
    }

    var uniqueSubscription = await dp.Kusto.ExecuteClusterQuery(GetUniqueSubscriptionQuery(detectorId, dataSource, timeRange));
    var uniqueResource = await dp.Kusto.ExecuteClusterQuery(GetUniqueResourceQuery(detectorId, dataSource, timeRange));
    string criticalInsightsCount = "0";
    long expandedTimes = 0;

    string deflectionCount = "0";
    string deflectionMonth = "";


    List<Task<DataTable>> deflectionTrendTasks = new List<Task<DataTable>>();
    List<Task<DataTable>> deflectionSolutionTasks = new List<Task<DataTable>>();

    Dictionary<string, Tuple<string, string, string, string>> supportTopicMapping = new Dictionary<string, Tuple<string, string, string, string>>();

    SupportTopic[] supportTopicList = null;
    if (cxt.ContainsKey("supportTopicList"))
    {
        // Id, PesId;
           supportTopicList  = cxt["supportTopicList"];
    }

    if(supportTopicList == null || supportTopicList.Length == 0)
    {
        deflectionCount = "N/A";
    }
    else
    {
        List<Task<DataTable>> deflectionTasks = new List<Task<DataTable>>();
        List<Task<DataTable>> supportTopicMapTasks = new List<Task<DataTable>>();

        foreach (var topic in supportTopicList)
        {
            var json = JsonConvert.SerializeObject(topic);

            // Example output: FullId, PesId, Id, TopicL2, TopicL3
           supportTopicMapTasks.Add(dp.Kusto.ExecuteClusterQuery(GetSupportTopicMapQuery(topic.Id, topic.PesId, timeRange)));
        }

        var supportTopicTasksList = await Task.WhenAll(supportTopicMapTasks);

       // supportTopicMapping holds the ID/PesId mapping to support topic L2/L3.
        if (supportTopicTasksList != null && supportTopicTasksList.Length > 0)
        {
            foreach (var table in supportTopicTasksList)
            {
                if (table != null && table.Rows != null && table.Rows.Count > 0)
                {
                    string supportTopicKey = table.Rows[0]["FullId"].ToString();
                    supportTopicMapping[table.Rows[0]["FullId"].ToString()] = new Tuple<string, string, string, string>(table.Rows[0]["PesId"].ToString(), table.Rows[0]["Id"].ToString(), table.Rows[0]["TopicL2"].ToString(), table.Rows[0]["TopicL3"].ToString());
                }
            }
        }

        foreach(var topic in supportTopicList)
        {
            var fullId = topic.PesId.ToString() + @"\" +  topic.Id.ToString();
            if (supportTopicMapping.ContainsKey(fullId))
            {
                deflectionTableName = GetDeflectionTable(isSolution, timeRange);
                deflectionSolutionTable = GetDeflectionTable(true, timeRange);
                deflectionTasks.Add(dp.Kusto.ExecuteClusterQuery(GetTotalDeflectionQuery(deflectionTableName, supportTopicMapping[fullId].Item1, supportTopicMapping[fullId].Item3, supportTopicMapping[fullId].Item4)));
                deflectionTrendTasks.Add(dp.Kusto.ExecuteClusterQuery(GetDeflectionBySuppportTopic(deflectionTableName, supportTopicMapping[fullId].Item1, supportTopicMapping[fullId].Item3, supportTopicMapping[fullId].Item4)));
                deflectionSolutionTasks.Add(dp.Kusto.ExecuteClusterQuery(GetDeflectionBySolution(deflectionSolutionTable, supportTopicMapping[fullId].Item1, supportTopicMapping[fullId].Item3, supportTopicMapping[fullId].Item4)));
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
                deflectionMonth = $"({deflectionTimeRange} : {timePeriod.ToString("MM/yy")})";
                if (deflectionTimeRange == "Week") {
                    deflectionMonth = $"(Last {deflectionTimeRange})";
                }
            }
        }
    }

    // AppInsights Table
    await dp.AppInsights.SetAppInsightsKey("73bff7df-297f-461e-8c14-377774ae7c12", "vkd6p42lgxcpeh04dzrwayp8zhhrfoeaxtcagube");
    var internalInsightsTable = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsQuery(detectorId, timeRange));
    var internalChildDetectors = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsQuery(detectorId, timeRange));
    var internalChildDetectorsExpand = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsExpandedQuery(detectorId, timeRange));
    var internalAllInsightsCount = await dp.AppInsights.ExecuteAppInsightsQuery(GetAllInsightsCount(detectorId, timeRange));
    var internalInsightsSummary = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsShowingAndClickQuery(detectorId, timeRange));
 
    await dp.AppInsights.SetAppInsightsKey("bda43898-4456-4046-9a7c-9ffa83f47c33", "2ejbz8ipv8uzgq14cjyqsimvh0hyjoxjcr7mpima");
    var externalInsightsTable = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsQuery(detectorId, timeRange));
    var externalChildDetectors = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsQuery(detectorId, timeRange));
    var externalChildDetectorsExpand = await dp.AppInsights.ExecuteAppInsightsQuery(GetChildDetectorsExpandedQuery(detectorId, timeRange));
    var externalAllInsightsCount = await dp.AppInsights.ExecuteAppInsightsQuery(GetAllInsightsCount(detectorId, timeRange));
    var externalInsightsSummary = await dp.AppInsights.ExecuteAppInsightsQuery(GetInsightsShowingAndClickQuery(detectorId, timeRange));

    expandedTimes = GetInsightsExpandedTimes(dataSource, externalInsightsTable, internalInsightsTable);
    

    string childDetectorsMappingMarkdown = GetChildDetectorsExpandedMapping(dataSource, internalChildDetectorsExpand, externalChildDetectorsExpand);



    string totalInsightsMarkdown= GetTotalInsightsMarkdown(dataSource, internalAllInsightsCount, externalAllInsightsCount, out criticalInsightsCount);

    var ds1 = new DataSummary($"Case Deflection {deflectionMonth}", $"{deflectionCount}", "yellowgreen");
    var ds2 = new DataSummary("Unique Subs", "0", "blue");
    var ds3 = new DataSummary("Unique Resources", "0", "yellow");
    // limegreen orangered yellow
    if (uniqueSubscription.Rows.Count > 0)
    {
        ds2 = new DataSummary("Unique Subs", uniqueSubscription.Rows[0][0].ToString(), "dodgerblue");
    }

    if (uniqueResource.Rows.Count > 0)
    {
        ds3 = new DataSummary("Unique Resources", uniqueResource.Rows[0][0].ToString(), "hotpink");
    }
    var ds4 = new DataSummary("Insights Expanded", expandedTimes.ToString(), "mediumpurple");
    var ds5 = new DataSummary("Critical Insights", criticalInsightsCount, "orangered");
    res.AddDataSummary(new List<DataSummary>() { ds1, ds2, ds3, ds4, ds5 });


    if(supportTopicList != null && supportTopicList.Length > 0) {

        var deflectionSolutionList = await Task.WhenAll(deflectionSolutionTasks);

        if (deflectionSolutionList != null && deflectionSolutionList.Length > 0)
        {
            foreach(var table in deflectionSolutionList)
            {
                var deflectionTrendTableRendering = new DiagnosticData()
                {
                    Table = table,
                    RenderingProperties = new Rendering(RenderingType.Table)
                };
                res.Dataset.Add(deflectionTrendTableRendering);
            }
        }
    }

    // Unique subscriptions and resources graph
    var usersandResourcesRangeTable = new DiagnosticData()
    {
        Table = await dp.Kusto.ExecuteClusterQuery(GetUserandResourceRangeQuery(detectorId, dataSource, timeRange, timeGrain)),
        //   RenderingProperties = new Rendering(RenderingType.Table)
        RenderingProperties = new TimeSeriesRendering()
        {
            Title = "Unique Subscriptions and Resources",
            GraphType = TimeSeriesType.BarGraph,
          //GraphType = TimeSeriesType.LineGraph,
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


    Dictionary<string, string> insightssummaryBody = new Dictionary<string, string>();
    insightssummaryBody.Add("Insights status", totalInsightsMarkdown);
 //   Insight allInsight = new Insight(InsightStatus.Success, "✨Insights status summary", insightssummaryBody, true);
//    res.AddInsight(allInsight);

    Dictionary<string, string> insightsBody = new Dictionary<string, string>();
    string markdownstr = GetAllCustomEventsQuery(dataSource, externalInsightsTable, internalInsightsTable);
    Dictionary<string, long> topInsights = GetTopInsightExpansion(dataSource, externalInsightsTable, internalInsightsTable);

    insightsBody.Add("Insights Ranking", markdownstr);
   // allInsight = new Insight(InsightStatus.Success, "💖 Top 5 expanded Insights", insightsBody, true);
  //  res.AddInsight(allInsight);
    markdownstr = "";

  //  Dictionary<string, string> insightsMappingBody = new Dictionary<string, string>();
      string insightsMappingMarkdown = GetInsightsExpandedMapping(dataSource, internalInsightsSummary, externalInsightsSummary, topInsights);
    markdownstr = insightsMappingMarkdown;
    if (!String.IsNullOrEmpty(markdownstr)) {
        insightssummaryBody.Add("Insights Summary", markdownstr);
    }
    Insight allInsight = new Insight(InsightStatus.Success, "💖 Insights Summary", insightssummaryBody, true);
    res.AddInsight(allInsight);
    markdownstr = "";
    
    Dictionary<string, string> childDetectorsBody = new Dictionary<string, string>();
    markdownstr = childDetectorsMappingMarkdown;
    childDetectorsBody.Add("Children Detectors", markdownstr);
    Insight allDetectors = new Insight(InsightStatus.Success, "✨ Children detectors summary ", childDetectorsBody, true);

    if (internalChildDetectors.Rows.Count > 0 || externalChildDetectors.Rows.Count > 0) {
        res.AddInsight(allDetectors);
    }

    // Not working part:

    if(supportTopicList != null && supportTopicList.Length > 0) {

        var deflectionTrendList = await Task.WhenAll(deflectionTrendTasks);

        if (deflectionTrendList != null && deflectionTrendList.Length > 0)
        {
            foreach(var table in deflectionTrendList)
            {
               int length = table.Rows.Count;
               int[] indexes = new int[]{0, length-1};
               if (length-1 > 0) {
                    for (int i = 0; i < indexes.Length; i++)
                    {
                        string value = "";

                        foreach (var col in table.Columns) {
                            value = !String.IsNullOrEmpty(table.Rows[i][col.ToString()].ToString()) ? table.Rows[i][col.ToString()].ToString() : "";
                        }   

                        foreach (var col in table.Columns) {
                            table.Rows[i][col.ToString()] = value;
                        }   
                    }
               }
  
                var deflectionTrendTableRendering = new DiagnosticData()
                {
                    Table = table,
                     //  RenderingProperties = new Rendering(RenderingType.Table)
                    RenderingProperties = new TimeSeriesRendering()
                    {
                        Title = "Deflection Trend",
                        GraphType = TimeSeriesType.LineGraph,
                        GraphOptions = new
                        {
                            color = new string[] { "dodgerblue", "hotpink", "#107E7D", "#8A2BE2", "#D2691E", "#008B8B", "#4298f4", "rgb(55, 175, 49)" },
                            forceY = new int[] { 0, 5 },
                            yAxis = new
                            {
                                axisLabel = "DeflectionTrend"
                            },
                            customizeX = "true"
                        }
                    }
                    // RenderingProperties = new Rendering(RenderingType.Table){
                    //     Title = "Exceptions"
                    // }
                };
                var deflectionTrendTableRendering1 = new DiagnosticData()
                {
                    Table = table,
                       RenderingProperties = new Rendering(RenderingType.Table)
                    // RenderingProperties = new TimeSeriesRendering()
                    // {
                    //     Title = "Deflection Trend",
                    //     GraphType = TimeSeriesType.LineGraph,
                    //     GraphOptions = new
                    //     {
                    //         color = new string[] { "dodgerblue", "hotpink", "#107E7D", "#8A2BE2", "#D2691E", "#008B8B", "#4298f4", "rgb(55, 175, 49)" },
                    //         forceY = new int[] { 0, 5 },
                    //         yAxis = new
                    //         {
                    //             axisLabel = "DeflectionTrend"
                    //         }
                    //     }
                    // }
                    // RenderingProperties = new Rendering(RenderingType.Table){
                    //     Title = "Exceptions"
                    // }
                };
                res.Dataset.Add(deflectionTrendTableRendering);
                res.Dataset.Add(deflectionTrendTableRendering1);
            }
        }
    }
   




    res.AddInsight(InsightStatus.Success, "⭐ Detector Rating coming soon");

    return res;
}

#region Deflection Metrics

private static string GetTotalDeflectionQuery(string tableName, string pesId, string category, string supportTopic)
{
    return $@"
    cluster('usage360').database('Product360').
    {tableName}
    | extend period = Timestamp
    | where period >= ago(60d)
    | where SupportTopicL2 =~ '{category}'
    | where SupportTopicL3 =~ '{supportTopic}'
    | where(DerivedProductIDStr == @'{pesId}')
    | where DenominatorQuantity != 0 
    | summarize qty = sum(NumeratorQuantity) / sum(DenominatorQuantity),Numerator = sum(NumeratorQuantity), Denominator = sum(DenominatorQuantity) by period
    | top 1 by period desc
    ";
}

private static string GetDeflectionBySolution(string tableName, string pesId, string category, string supportTopic)
{
    return $@"
    cluster('usage360').database('Product360').
    {tableName}
    | extend Current = CurrentDenominatorQuantity, Previous = PreviousDenominatorQuantity, PreviousN = PreviousNDenominatorQuantity , CurrentQ = CurrentNumeratorQuantity, PreviousQ = PreviousNumeratorQuantity, PreviousNQ = PreviousNNumeratorQuantity,  Change = CurrentNumeratorQuantity-PreviousNumeratorQuantity
    | extend C_ID = SolutionType, C_Name = SolutionType
    | where SupportTopicL2 contains '{category}'
    | where SupportTopicL3 contains '{supportTopic}'
    | where (DerivedProductIDStr == @'{pesId}')
    | where C_ID != ''
    | summarize C_Name=any(C_Name), Current= sum(Current), Previous = sum(Previous), PreviousN = sum(PreviousN), CurrentQ = sum(CurrentNumeratorQuantity), PreviousQ = sum(PreviousNumeratorQuantity), PreviousNQ = sum(PreviousNNumeratorQuantity) by C_ID | extend Change = Current - Previous
    | extend CurPer = iff(Current == 0, todouble(''), CurrentQ/Current), PrevPer = iff(Previous == 0, todouble(''), PreviousQ/Previous), NPrevPer = iff(PreviousN == 0, todouble(''), PreviousNQ/PreviousN)
    | order by Current desc, Previous desc, PreviousN desc
    | limit 100
    | project C_ID, C_Name = iif(isempty(C_Name),C_ID,C_Name), Current, CurPer, Previous, PrevPer, Change, PreviousN, NPrevPer
    | order by Current desc, Previous desc, PreviousN desc
    | project Id = C_ID, Name = C_Name, Current = CurPer, CurrentNumerator = CurPer * Current, CurrentDenominator = round(Current, 2), Previous = PrevPer, PreviousNumerator = PrevPer * Previous, PreviousDenominator = round(Previous,2), Change, 12WeeksPrior = NPrevPer
    | project Name , CurrentDeflection = round(Current*100.0,2),  CurrentNumerator, CurrentDenominator , PreviousDeflection = round(Previous *100.0,2),  PreviousNumerator, PreviousDenominator
    ";
}

private static string GetDeflectionBySuppportTopic1(string tableName, string pesId, string category, string supportTopic)
{
    return $@"
    cluster('usage360').database('Product360').
    {tableName}
    | where Timestamp >= ago(150d)
    | where DerivedProductIDStr in ('{pesId}')
    | where SupportTopicL2 contains '{category}'
    | where SupportTopicL3 contains '{supportTopic}'
    | where DenominatorQuantity != 0 
    | summarize qty = sum(NumeratorQuantity) / sum(DenominatorQuantity), auxQty = sum(DenominatorQuantity) by Timestamp, ProductName
    | project Timestamp , deflection = round(100 * qty, 2)
    | sort by Timestamp asc
    ";
}

private static string GetDeflectionBySuppportTopic(string tableName, string pesId, string category, string supportTopic)
{
    return $@"
    cluster('usage360').database('Product360').
    SupportProductionDeflectionWeeklyVer1023
    | where Timestamp >= ago(150d)
    | where DerivedProductIDStr in ('{pesId}')
    | where SupportTopicL2 contains '{category}'
    | where SupportTopicL3 contains '{supportTopic}'
    | where DenominatorQuantity != 0 
    | summarize qty = sum(NumeratorQuantity) / sum(DenominatorQuantity), auxQty = sum(DenominatorQuantity) by Timestamp, ProductName
    | project Timestamp , WeeklyDeflection = round(100 * qty, 2)
    | sort by Timestamp asc
    | union (
        cluster('usage360').database('Product360').
        SupportProductionDeflectionMonthlyVer1023
        | where Timestamp >= ago(150d)
        | where DerivedProductIDStr in ('{pesId}')
        | where SupportTopicL2 contains '{category}'
        | where SupportTopicL3 contains '{supportTopic}'
        | where DenominatorQuantity != 0 
        | summarize qty = sum(NumeratorQuantity) / sum(DenominatorQuantity), auxQty = sum(DenominatorQuantity) by Timestamp, ProductName
        | project Timestamp , MonthlyDeflection = round(100 * qty, 2)
        | sort by Timestamp asc
    )
    | sort by Timestamp asc
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