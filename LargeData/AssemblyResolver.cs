using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http.Dispatcher;

namespace LargeData
{
    // add a reference in GlobalConfiguration
    // GlobalConfiguration.Configuration.Services.Replace(typeof(IAssembliesResolver), new AssemblyResolver());
    public class AssemblyResolver : DefaultAssembliesResolver
    {
        public override ICollection<Assembly> GetAssemblies()
        {
            List<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().ToList();
            var controllersAssembly = Assembly.LoadFrom(Assembly.GetExecutingAssembly().Location);
            assemblies.Add(controllersAssembly);
            return assemblies;
        }
    }
}
