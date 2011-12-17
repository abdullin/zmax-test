// Copyright (c) Rinat Abdullin 2011.
// Free use of this software is granted under the terms 
// of the GNU Lesser General Public License (LGPL)

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ZMQ;
namespace zmax_test
{
    class Program
    {
        static void Main(string[] args)
        {
            var t = new CancellationTokenSource();
            var ctx = new Context(1);

            var client = Task.Factory.StartNew(() =>
                {
                    using (var req = ctx.Socket(SocketType.REP))
                    {
                        req.Bind("tcp://*:8081");
                        while(true)
                        {
                            req.RecvAll();
                            req.Send(Guid.NewGuid().ToByteArray());
                        }
                    }
                });
            uint lmaxCounter = 0;

            var lmaxUnmarshaller = Task.Factory.StartNew(() =>
                {
                    using (var req = ctx.Socket(SocketType.REQ))
                    using (var f = File.Create("queue"))
                    {
                        f.SetLength(1024*1024*50);
                        req.Connect("tcp://localhost:8081");


                        while(!t.IsCancellationRequested)
                        {
                            req.Send(new byte[]{1});// ready
                            var bytes = req.Recv();
                            //f.Write(bytes,0, bytes.Length);
                            lmaxCounter += 1;
                            //f.Flush();
                        }
                    }
                });

            while(true)
            {
                Thread.Sleep(1000);
                Console.WriteLine(new
                    {
                        lmaxCounter
                    });
            }
        }
    }
}