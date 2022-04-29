using MISA.Meinvoice.Kinesis.Consumer.Library;
using System;
using System.Threading;

namespace Test_console
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            Run();
        }

        public static void Run()
        {
            while (ProcessNextLine())
            {
            }
        }

        private static bool ProcessNextLine()
        {
            Thread.Sleep(3000);
            return true;
        }
    }
}
