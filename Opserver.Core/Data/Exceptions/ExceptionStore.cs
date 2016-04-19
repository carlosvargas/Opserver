using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Profiling;
using StackExchange.Exceptional;
using StackExchange.Opserver.Helpers;

namespace StackExchange.Opserver.Data.Exceptions
{
    public class ExceptionStore : PollNode
    {
        public const int PerAppSummaryCount = 1000;

        private int? QueryTimeout => Settings.QueryTimeoutMs;
        public string Name => Settings.Name;
        public string Description => Settings.Description;
        public ExceptionsSettings.Store Settings { get; internal set; }

        public override int MinSecondsBetweenPolls => 1;
        public override string NodeType => "Exceptions";

        public override IEnumerable<Cache> DataPollers
        {
            get
            {
                yield return Applications;
                yield return ErrorSummary;
            }
        }

        protected override IEnumerable<MonitorStatus> GetMonitorStatus() { yield break; }
        protected override string GetMonitorStatusReason() { return null; }
        
        public ExceptionStore(ExceptionsSettings.Store settings) : base(settings.Name)
        {
            Settings = settings;
        }

        public Action<Cache<T>> UpdateFromSql<T>(string opName, Func<Task<T>> getFromConnection) where T : class
        {
            return UpdateCacheItem<T>("Exceptions Fetch: " + Name + ":" + opName,
                                   getFromConnection,
                                   addExceptionData: e => e.AddLoggedData("Server", Name));
        }

        private Cache<List<Application>> _applications;
        public Cache<List<Application>> Applications
        {
            get
            {
                return _applications ?? (_applications = new Cache<List<Application>>
                    {
                        CacheForSeconds = Settings.PollIntervalSeconds,
                        UpdateCache = UpdateFromSql("Applications-List",
                            async () =>
                            {
                                var result = (await QueryListAsync<Application>($"Applications Fetch: {Name}", @"
Select 'API' as Name, 
       COUNT(ID) as ExceptionCount,
	   0 AS RecentExceotionCount,
	   MAX(Date) as MostRecent
  From APIErrors", new {Current.Settings.Exceptions.RecentSeconds}));
                                result.ForEach(a => { a.StoreName = Name; a.Store = this; });
                                return result;
                         })
                    });
            }
        }

        private Cache<List<Error>> _errorSummary;
        public Cache<List<Error>> ErrorSummary
        {
            get
            {
                return _errorSummary ?? (_errorSummary = new Cache<List<Error>>
                {
                    CacheForSeconds = Settings.PollIntervalSeconds,
                    UpdateCache = UpdateFromSql("Error-Summary-List",
                        () => QueryListAsync<Error>($"ErrorSummary Fetch: {Name}", @"
Select e.ID AS Id, e.GUID, 'API' AS ApplicationName, e.MachineName, e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
  From APIErrors as e
 Order By Date Desc", new { PerAppSummaryCount }))
                });
            }
        }

        public List<Error> GetErrorSummary(int maxPerApp, string appName = null)
        {
            var errors = ErrorSummary.SafeData(true);
            // specific application
            if (appName.HasValue())
            {
                return errors.Where(e => e.ApplicationName == appName)
                             .Take(maxPerApp)
                             .ToList();
            }
            // all apps, 1000
            if (maxPerApp == PerAppSummaryCount)
            {
                return errors;
            }
            // app apps, n records
            return errors.GroupBy(e => e.ApplicationName)
                         .SelectMany(e => e.Take(maxPerApp))
                         .ToList();
        }

        /// <summary>
        /// Get all current errors, possibly per application
        /// </summary>
        /// <remarks>This does not populate Detail, it's comparatively large and unused in list views</remarks>
        public Task<List<Error>> GetAllErrors(int maxPerApp, string appName = null)
        {
            return QueryListAsync<Error>($"GetAllErrors() for {Name} App: {(appName ?? "All")}", @"
Select TOP (@maxPerApp) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
  From APIErrors as e
 Order By Date Desc", new {maxPerApp, appName});
        }

        public Task<List<Error>> GetSimilarErrors(Error error, int max)
        {
            return QueryListAsync<Error>($"GetSimilarErrors() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     WHERE Message = @Message
     Order By Date Desc", new {max, error.ApplicationName, error.Message});
        }

        public Task<List<Error>> GetSimilarErrorsInTime(Error error, int max)
        {
            return QueryListAsync<Error>($"GetSimilarErrorsInTime() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     Where Date Between @start and @end
     Order By Date Desc", new { max, start = error.CreationDate.AddMinutes(-5), end = error.CreationDate.AddMinutes(5) });
        }

        public Task<List<Error>> FindErrors(string searchText, string appName, int max, bool includeDeleted)
        {
            return QueryListAsync<Error>($"FindErrors() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     Where (Message Like @search Or request Like @search Or Url Like @search)
     Order By Date Desc", new { search = '%' + searchText + '%', appName, max });
        }

        public Task<int> DeleteAllErrors(string appName)
        {
            return ExecTask($"DeleteAllErrors() (app: {appName}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where DeletionDate Is Null 
   And IsProtected = 0 
   And ApplicationName = @appName", new { appName });
        }

        public Task<int> DeleteSimilarErrors(Error error)
        {
            return ExecTask($"DeleteSimilarErrors('{error.GUID}') (app: {error.ApplicationName}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where ApplicationName = @ApplicationName
   And Message = @Message
   And DeletionDate Is Null
   And IsProtected = 0", new {error.ApplicationName, error.Message});
        }

        public Task<int> DeleteErrors(string appName, List<Guid> ids)
        {
            return ExecTask($"DeleteErrors({ids.Count} Guids) (app: {appName}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where DeletionDate Is Null 
   And IsProtected = 0 
   And ApplicationName = @appName
   And GUID In @ids", new { appName, ids });
        }
        
        public async Task<Error> GetError(Guid guid)
        {
            try
            {
                Error sqlError;
                using (MiniProfiler.Current.Step("GetError() (guid: " + guid + ") for " + Name))
                using (var c = await GetConnectionAsync())
                {
                    sqlError = (await c.QueryAsync<Error>(@"
    Select Top 1 e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request AS FullJson, e.Message, e.StatusCode, '', 0 AS DuplicationCount, e.Exception as Detail
      From ApiErrors e
     Where GUID = @guid", new { guid }, commandTimeout: QueryTimeout)).FirstOrDefault();
                }
                if (sqlError == null) return null;

                // everything is in the JSON, but not the columns and we have to deserialize for collections anyway
                // so use that deserialized version and just get the properties that might change on the SQL side and apply them
                var result = sqlError;
                result.DuplicateCount = sqlError.DuplicateCount;
                result.DeletionDate = sqlError.DeletionDate;
                result.ApplicationName = sqlError.ApplicationName;
                return result;
            }
            catch (Exception e)
            {
                Current.LogException(e);
                return null;
            }
        }

        public async Task<bool> ProtectError(Guid guid)
        {
              return await ExecTask($"ProtectError() (guid: {guid}) for {Name}", @"
Update Exceptions 
   Set IsProtected = 1, DeletionDate = Null
 Where GUID = @guid", new {guid}) > 0;
        }

        public async Task<bool> DeleteError(Guid guid)
        {
            return await ExecTask($"DeleteError() (guid: {guid}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where GUID = @guid 
   And DeletionDate Is Null", new { guid }) > 0;
        }

        public async Task<List<T>> QueryListAsync<T>(string step, string sql, dynamic paramsObj)
        {
            try
            {
                using (MiniProfiler.Current.Step(step))
                using (var c = await GetConnectionAsync())
                {
                    return await c.QueryAsync<T>(sql, paramsObj as object, commandTimeout: QueryTimeout);
                }
            }
            catch (Exception e)
            {
                Current.LogException(e);
                return new List<T>();
            }
        }

        public async Task<int> ExecTask(string step, string sql, dynamic paramsObj)
        {
            using (MiniProfiler.Current.Step(step))
            using (var c = await GetConnectionAsync())
            {
                return await c.ExecuteAsync(sql, paramsObj as object, commandTimeout: QueryTimeout);
            }
        }

        private Task<DbConnection> GetConnectionAsync()
        {
            return Connection.GetOpenAsync(Settings.ConnectionString, QueryTimeout);
        }
    }
}