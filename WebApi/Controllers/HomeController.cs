using System.Net.Http;
using System.Web.Http;

namespace WebApi.Controllers
{
    public class HomeController : ApiController
    {
        [HttpGet]
        public HttpResponseMessage HelloWorld()
        {
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent("Hello World")
            };
        }
    }
}
