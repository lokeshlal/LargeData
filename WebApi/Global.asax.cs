﻿using LargeData;
using System.Web;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Mvc;
using System.Web.Routing;

namespace WebApi
{
    public class WebApiApplication : HttpApplication
    {
        protected void Application_Start()
        {
            GlobalConfiguration.Configure(WebApiConfig.Register);
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);

            // include the large data controllers
            GlobalConfiguration.Configuration.Services.Replace(typeof(IAssembliesResolver), new AssemblyResolver());
            // provide a callback which will accept filters and give back the dataset
            // uncomment to test dataset
            //ServerSettings.Callback = DataProvider.GetDataForDownload;
            // uncomment to test data reader
            ServerSettings.CallbackReader = DataProvider.GetDataReaderForDownload;
            
            // callback to handle uploaded datasets
            //ServerSettings.CallbackUpload = DataReciever.AcceptDataSet;
            ServerSettings.CallbackUploadReader = DataReciever.AcceptDataReader;


            ServerSettings.MaxRecordsInAFile = 10;
            ServerSettings.TemporaryLocation = @"E:\TempLocation";
            ServerSettings.MaxFileSize = 10;
        }
    }
}
