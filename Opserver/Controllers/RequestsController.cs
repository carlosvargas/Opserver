using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using StackExchange.Opserver.Data.Exceptions;
using StackExchange.Opserver.Helpers;
using StackExchange.Opserver.Models;
using StackExchange.Opserver.Views.Exceptions;
using System.Threading.Tasks;
using Microsoft.Ajax.Utilities;
using StackExchange.Opserver.Data.Jira;
using static StackExchange.Opserver.Data.Exceptions.ExceptionStores;

namespace StackExchange.Opserver.Controllers
{
	using Data;

	[OnlyAllow(Roles.Exceptions)] 
    public class RequestsController : ExceptionsController
    {

        public override TopTab TopTab => new TopTab("Requests", "Exceptions", this, 40)
        {
            GetMonitorStatus = () =>  MonitorStatus.Good,
            GetBadgeCount = () => 0,
            GetTooltip = () => TotalRecentExceptionCount.ToComma() + " recent"
        };

        [Route("requests")]
        public ActionResult Exceptions(string group, string log, ExceptionSorts? sort = null, int? count = null)
        {
            return base.Exceptions(group, log, sort, count, false);
        }

        [Route("requests/load-more")]
        public ActionResult LoadMore(string group, string log, ExceptionSorts sort, int? count = null, Guid? prevLast = null)
        {
	        return base.LoadMore(group, log, sort, count, prevLast, false);
        }

		[Route("requests/detail")]
		public async Task<ActionResult> Detail(string app, Guid id)
		{
			return await base.Detail(app, id, false);
		}

		[Route("requests/preview")]
		public async Task<ActionResult> Preview(string app, Guid id)
		{
			return await base.Preview(app, id, false);
		}

		[Route("requests/detail/json"), AlsoAllow(Roles.Anonymous)]
        public async Task<JsonResult> DetailJson(string app, Guid id)
		{
			return await base.DetailJson(app, id, false);
        }

        [Route("requests/counts")]
        public ActionResult Counts()
        {
            var groups = ApplicationGroups.Select(g => new
            {
                g.Name,
                g.Total,
                Applications = g.Applications.Select(a => new
                {
                    a.Name,
                    Total = a.ExceptionCount
                })
            });
            return Json(new
            {
                Groups = groups,
                Total = TotalExceptionCount
            });
        }
    }
}