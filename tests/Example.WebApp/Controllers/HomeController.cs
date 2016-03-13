using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Hangfire;

namespace Example.WebApp.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult About()
        {
            BackgroundJob.Schedule(() => HomeController.DummyTask(), TimeSpan.FromMinutes(2));

            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            BackgroundJob.Enqueue(() => HomeController.DummyTask());

            ViewBag.Message = "Your contact page.";

            return View();
        }

        public static bool DummyTask()
        {
            return IsPrime((new Random()).Next((int) (int.MaxValue*0.99), int.MaxValue));
        }

        private static bool IsPrime(int candidate)
        {
            if ((candidate & 1) == 0)
                return candidate == 2;

            for (var i = 3; (i * i) <= candidate; i += 2)
            {
                if ((candidate % i) == 0)
                {
                    return false;
                }
            }

            return candidate != 1;
        }
    }
}