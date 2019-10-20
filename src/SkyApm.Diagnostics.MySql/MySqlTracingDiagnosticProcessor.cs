using MySql.Data.MySqlClient;
using SkyApm.Tracing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace SkyApm.Diagnostics.MySql
{
    public class MySqlTracingDiagnosticProcessor : DefaultTraceListener, ITracingDiagnosticProcessor
    {
        private readonly ITracingContext _tracingContext;
        private readonly IExitSegmentContextAccessor _contextAccessor;

        private Dictionary<long, MySqlMetaInfo> _metaInfoCache = new Dictionary<long, MySqlMetaInfo>();

        public string ListenerName => MySqlDiagnosticStrings.DiagnosticListenerName;

        public MySqlTracingDiagnosticProcessor(
            ITracingContext tracingContext,
            IExitSegmentContextAccessor contextAccessor)
        {
            _tracingContext = tracingContext;
            _contextAccessor = contextAccessor;
            //to use this,you should add "logging=true" to the connection string
            MySqlTrace.Listeners.Clear();
            MySqlTrace.Listeners.Add(this);
            MySqlTrace.Switch.Level = SourceLevels.Information;
            MySqlTrace.QueryAnalysisEnabled = true;
        }

        public void BeforeSqlExecute(MySqlMetaInfo metaInfo, string sql)
        {
            if (metaInfo == null || string.IsNullOrEmpty(sql))
            {
                return;
            }
            var opration = sql.Split(' ').First();
            var context = _tracingContext.CreateExitSegmentContext(opration, metaInfo.DbServer);
            context.Span.SpanLayer = Tracing.Segments.SpanLayer.DB;
            context.Span.Component = Common.Components.MYSQL;
            context.Span.AddTag(Common.Tags.DB_TYPE, "MySql");
            context.Span.AddTag(Common.Tags.DB_INSTANCE, metaInfo.DbName);
            context.Span.AddTag(Common.Tags.DB_STATEMENT, sql);
        }

        public void AfterSqlExecute()
        {
            var context = _contextAccessor.Context;
            if (context != null)
            {
                _tracingContext.Release(context);
            }
        }

        public void ErrorSqlExecute(string errorMsg)
        {
            var context = _contextAccessor.Context;
            if (context != null)
            {
                context.Span?.ErrorOccurred(new Exception(errorMsg));
            }
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string format, params object[] args)
        {
            switch ((MySqlTraceEventType)id)
            {
                case MySqlTraceEventType.ConnectionOpened:
                    var conStr = args[1].ToString();
                    var conStrBuilder = new MySqlConnectionStringBuilder(conStr);
                    var metaInfo = new MySqlMetaInfo
                    {
                        ConnectionString = conStr,
                        DbName = conStrBuilder.Database,
                        DbServer = conStrBuilder.Server
                    };
                    CacheMySqlMetaInfo((long)args[0], metaInfo);
                    break;

                case MySqlTraceEventType.ConnectionClosed:
                    break;

                case MySqlTraceEventType.QueryOpened:
                    string sql = args[2]?.ToString();
                    BeforeSqlExecute(GetMySqlMetaInfo((long)args[0]), sql);
                    break;

                case MySqlTraceEventType.ResultOpened:
                    break;

                case MySqlTraceEventType.ResultClosed:
                    break;

                case MySqlTraceEventType.QueryClosed:
                    AfterSqlExecute();
                    break;

                case MySqlTraceEventType.StatementPrepared:
                    break;

                case MySqlTraceEventType.StatementExecuted:
                    break;

                case MySqlTraceEventType.StatementClosed:
                    break;

                case MySqlTraceEventType.NonQuery:
                    break;

                case MySqlTraceEventType.UsageAdvisorWarning:
                    break;

                case MySqlTraceEventType.Warning:
                    break;

                case MySqlTraceEventType.Error:
                    ErrorSqlExecute(args[2]?.ToString());
                    break;

                case MySqlTraceEventType.QueryNormalized:
                    break;
            }
        }

        private void CacheMySqlMetaInfo(long key, MySqlMetaInfo metaInfo)
        {
            if (!_metaInfoCache.ContainsKey(key))
            {
                _metaInfoCache.Add(key, metaInfo);
            }
        }

        private MySqlMetaInfo GetMySqlMetaInfo(long key)
        {
            if (_metaInfoCache.ContainsKey(key))
            {
                return _metaInfoCache[key];
            }
            return null;
        }
    }
}