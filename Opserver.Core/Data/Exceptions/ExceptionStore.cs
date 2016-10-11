using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using StackExchange.Profiling;
using StackExchange.Exceptional;
using StackExchange.Opserver.Helpers;

namespace StackExchange.Opserver.Data.Exceptions
{
    public class ExceptionStore : PollNode
    {
        public const int PerAppSummaryCount = 1000;

        public override string ToString() => "Store: " + Settings.Name;

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

        public Func<Cache<T>, Task> UpdateFromSql<T>(string opName, Func<Task<T>> getFromConnection, Action<Cache<T>> afterPoll = null) where T : class
        {
            return UpdateCacheItem("Exceptions Fetch: " + Name + ":" + opName,
                getFromConnection,
                addExceptionData: e => e.AddLoggedData("Server", Name),
                afterPoll: afterPoll);
        }

        private Cache<List<Application>> _applications;
        public Cache<List<Application>> Applications
        {
            get
            {
                return _applications ?? (_applications = new Cache<List<Application>>
                    {
                        CacheForSeconds = Settings.PollIntervalSeconds,
                        UpdateCache = UpdateFromSql(nameof(Applications),
                            async () =>
                            {
                                var result = await QueryListAsync<Application>($"Applications Fetch: {Name}", @"
Select 'API' as Name, 
       COUNT(ID) as ExceptionCount,
	   0 AS RecentExceptionCount,
	   MAX(Date) as MostRecent
  From APIErrors
Where Level = 'ERROR'", new {Current.Settings.Exceptions.RecentSeconds}).ConfigureAwait(false);
                                result.ForEach(a => { a.StoreName = Name; a.Store = this; });
                                return result;
                               },
                               afterPoll: cache => ExceptionStores.UpdateApplicationGroups())
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
                    UpdateCache = UpdateFromSql(nameof(ErrorSummary),
                        () => QueryListAsync<Error>($"ErrorSummary Fetch: {Name}", @"
Select e.ID AS Id, e.GUID, 'API' AS ApplicationName, e.MachineName, e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
  From APIErrors as e
 Where Level = 'ERROR'
 Order By Date Desc", new { PerAppSummaryCount }),
                               afterPoll: cache => ExceptionStores.UpdateApplicationGroups())
                       });
            }
        }

        public List<Error> GetErrorSummary(int maxPerApp, string group = null, string app = null)
        {
            var errors = ErrorSummary.Data;
            if (errors == null) return new List<Error>();
            // specific application - this is most specific
            if (app.HasValue())
            {
                return errors.Where(e => e.ApplicationName == app)
                             .Take(maxPerApp)
                             .ToList();
            }
            if (group.HasValue())
            {
                var apps = ExceptionStores.GetAppNames(group, app).ToHashSet();
                return errors.GroupBy(e => e.ApplicationName)
                             .Where(g => apps.Contains(g.Key))
                             .SelectMany(e => e.Take(maxPerApp))
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
        public Task<List<Error>> GetAllErrorsAsync(int maxPerApp, IEnumerable<string> apps = null)
        {
            return QueryListAsync<Error>($"{nameof(GetAllErrorsAsync)}() for {Name}", @"
Select TOP (@maxPerApp) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
  From APIErrors as e
 Where Level = 'ERROR'
 Order By Date Desc", new {maxPerApp, apps});
        }

        public Task<List<Error>> GetSimilarErrorsAsync(Error error, int max)
        {
            return QueryListAsync<Error>($"{nameof(GetSimilarErrorsAsync)}() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     WHERE Message = @Message
     Order By Date Desc", new {max, error.ApplicationName, error.Message});
        }

        public Task<List<Error>> GetSimilarErrorsInTimeAsync(Error error, int max)
        {
            return QueryListAsync<Error>($"{nameof(GetSimilarErrorsInTimeAsync)}() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     Where Date Between @start and @end
     Order By Date Desc", new { max, start = error.CreationDate.AddMinutes(-5), end = error.CreationDate.AddMinutes(5) });
        }

        public Task<List<Error>> FindErrorsAsync(string searchText, int max, bool includeDeleted, IEnumerable<string> apps = null)
        {
            return QueryListAsync<Error>($"{nameof(FindErrorsAsync)}() for {Name}", @"
	Select TOP (@max) e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request, e.Message, e.StatusCode, '', 0
	  From APIErrors as e
     Where (Message Like @search Or request Like @search Or Url Like @search)
     Order By Date Desc", new { search = '%' + searchText + '%', apps, max });
        }

        public Task<int> DeleteAllErrorsAsync(List<string> apps)
        {
            return ExecTaskAsync($"{nameof(DeleteAllErrorsAsync)}() for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where DeletionDate Is Null 
   And IsProtected = 0 
   And ApplicationName In @apps", new { apps });
        }

        public Task<int> DeleteSimilarErrorsAsync(Error error)
        {
            return ExecTaskAsync($"{nameof(DeleteSimilarErrorsAsync)}('{error.GUID.ToString()}') (app: {error.ApplicationName}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where ApplicationName = @ApplicationName
   And Message = @Message
   And DeletionDate Is Null
   And IsProtected = 0", new {error.ApplicationName, error.Message});
        }

        public Task<int> DeleteErrorsAsync(List<Guid> ids)
        {
            return ExecTaskAsync($"{nameof(DeleteErrorsAsync)}({ids.Count.ToString()} Guids) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where DeletionDate Is Null 
   And IsProtected = 0 
   And GUID In @ids", new { ids });
        }
        
        public async Task<Error> GetErrorAsync(Guid guid)
        {
            try
            {
                Error sqlError;
                using (MiniProfiler.Current.Step(nameof(GetErrorAsync) + "() (guid: " + guid.ToString() + ") for " + Name))
                using (var c = await GetConnectionAsync().ConfigureAwait(false))
                {
                    sqlError = await c.QueryFirstOrDefaultAsync<Error>(@"
    Select Top 1 e.ID AS Id, e.GUID, 'API' AS ApplicationName, COALESCE(e.MachineName, ''), e.Date As CreationDate, '', '', e.Host, e.Url, e.HTTPMethod, e.IPAddress, e.Request AS FullJson, e.Message, e.StatusCode, '', 0 AS DuplicationCount, e.Exception as Detail
      From ApiErrors e
     Where GUID = @guid AND Level = 'ERROR'", new { guid }, commandTimeout: QueryTimeout).ConfigureAwait(false);
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

        public async Task<bool> ProtectErrorAsync(Guid guid)
        {
              return await ExecTaskAsync($"{nameof(ProtectErrorAsync)}() (guid: {guid.ToString()}) for {Name}", @"
Update Exceptions 
   Set IsProtected = 1, DeletionDate = Null
 Where GUID = @guid", new {guid}).ConfigureAwait(false) > 0;
        }

        public async Task<bool> DeleteErrorAsync(Guid guid)
        {
            return await ExecTaskAsync($"{nameof(DeleteErrorAsync)}() (guid: {guid.ToString()}) for {Name}", @"
Update Exceptions 
   Set DeletionDate = GETUTCDATE() 
 Where GUID = @guid 
   And DeletionDate Is Null", new { guid }).ConfigureAwait(false) > 0;
        }

        public async Task<List<T>> QueryListAsync<T>(string step, string sql, dynamic paramsObj)
        {
            try
            {
                using (MiniProfiler.Current.Step(step))
                using (var c = await GetConnectionAsync().ConfigureAwait(false))
                {
                    return await c.QueryAsync<T>(sql, paramsObj as object, commandTimeout: QueryTimeout).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                Current.LogException(e);
                return new List<T>();
            }
        }

        public async Task<int> ExecTaskAsync(string step, string sql, dynamic paramsObj)
        {
            using (MiniProfiler.Current.Step(step))
            using (var c = await GetConnectionAsync().ConfigureAwait(false))
            {
                return await c.ExecuteAsync(sql, paramsObj as object, commandTimeout: QueryTimeout).ConfigureAwait(false);
            }
        }

        private Task<DbConnection> GetConnectionAsync() =>
            Connection.GetOpenAsync(Settings.ConnectionString, QueryTimeout);
    }
}