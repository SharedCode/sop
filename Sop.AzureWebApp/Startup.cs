using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(Sop.AzureWebApp.Startup))]
namespace Sop.AzureWebApp
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
