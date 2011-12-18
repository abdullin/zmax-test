// Copyright (c) Rinat Abdullin 2011.
// Free use of this software is granted under the terms 
// of the GNU Lesser General Public License (LGPL)

using System;
using System.Diagnostics;
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

			int[] clientPosition = {0};
			const int numberOfClients = 5;

			for (int i = 0; i < numberOfClients; i++)
			{
				int i1 = i;
				Task.Factory.StartNew(() =>
				{
					Thread.Sleep(200+ 100*i1);
					using (var req = ctx.Socket(SocketType.REQ))
					{
						req.Connect("tcp://192.168.1.1:8081");
						while (!t.Token.IsCancellationRequested)
						{

							req.Send(Guid.NewGuid().ToByteArray());
							req.Recv();
							clientPosition[0] += 1;
						}
					}
				});
			}

			// normally between the clients and store there will be
			// majordomo (for load balancing and partitioning but we ignore it for now)

			int storePosition = 0;
			var positions = new long[64000 * 16];
			var store = Task.Factory.StartNew(() =>
				{
					using (var req = ctx.Socket(SocketType.REP))
					using (var f = File.Open("queue", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
					{
						f.SetLength(1024 * 1024 * 50);
						req.Bind("tcp://192.168.1.1:8081");
						while (!t.Token.IsCancellationRequested)
						{
							var bytes = req.Recv();
							req.Send(new byte[0]); // ready
							
							f.Write(bytes, 0, bytes.Length);

							positions[storePosition] = f.Position;
							storePosition += 1;
							f.Flush();
						}
					}
				});

			var publisherIdx = 0;


			var publish = Task.Factory.StartNew(() =>
				{
					using (var pub = ctx.Socket(SocketType.PUB))
					{
						pub.Bind("pgm://192.168.1.1:8082");
						Thread.Sleep(2000);
						using (var f = File.Open("queue", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
						{
							var buffer = new byte[16];

							while (true)
							{
								while (publisherIdx < storePosition)
								{
									f.Read(buffer, 0, 16);
									pub.Send(buffer);
									publisherIdx += 1;
								}

								Thread.Sleep(1);
							}
						}
					}
				});
			int handlerIdx = 0;

			var eventHandler = Task.Factory.StartNew(() =>
				{
					using (var eh = ctx.Socket(SocketType.SUB))
					{
						eh.Connect("pgm://192.168.1.1:8082");
						eh.Subscribe(new byte[0]);
						while (true)
						{
							eh.Recv();
							handlerIdx += 1;
						}
					}
				});


			Thread.Sleep(1000);
			Console.WriteLine(
				@"Configuration:
Client => Unmarshaller + Persister: TCP
          Publisher               : UDP  => Handler 1
                                         => Handler 2


Average guaranteed throughput from 1 client to handlers

Client         Event Store     Event Handler
");



			for (int i = 0; i < int.MaxValue; i++)
			{
				var watch = Stopwatch.StartNew();
				int handlerStart = handlerIdx;
				int clientStart = clientPosition[0];
				var storeStart = storePosition;

				Thread.Sleep(1000);
				var totalSeconds = watch.Elapsed.TotalSeconds;
				var storeMps = Math.Round((storePosition - storeStart) / totalSeconds);

				var handlerMps = Math.Round((handlerIdx - handlerStart) / totalSeconds);
				var clientMps = Math.Round((clientPosition[0] - clientStart) / totalSeconds);

				Console.WriteLine("{0:00000}          {1:00000}           {2:00000} (msg per second)", clientMps, storeMps, handlerMps);
				if (i == 11)
				{
					Console.WriteLine("Stop sending");
					t.Cancel();
				}
					
			}

			Console.ReadLine();
			
			while (true) {}
		}
	}
}