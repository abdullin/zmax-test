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
			var ctx = new Context(2);

			int cliendIdx = 0;
			var client = Task.Factory.StartNew(() =>
				{
					Thread.Sleep(200);
					using (var req = ctx.Socket(SocketType.REP))
					{
						req.Bind("tcp://*:8081");
						while (!t.Token.IsCancellationRequested)
						{
							req.Recv();
							req.Send(Guid.NewGuid().ToByteArray());
							cliendIdx += 1;
						}
					}
				});
			int unmarshallerIdx = 0;

			var positions = new long[64000 * 8];


			var unmarshaller = Task.Factory.StartNew(() =>
				{
					using (var req = ctx.Socket(SocketType.REQ))
					using (var f = File.Open("queue", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
					{
						f.SetLength(1024 * 1024 * 50);
						req.Connect("tcp://192.168.1.1:8081");
						while (!t.Token.IsCancellationRequested)
						{
							req.Send(new byte[] {1}); // ready
							var bytes = req.Recv();
							f.Write(bytes, 0, bytes.Length);

							positions[unmarshallerIdx] = f.Position;
							unmarshallerIdx += 1;
							f.Flush();
						}
					}
				});

			var publisherIdx = 0;


			var publish = Task.Factory.StartNew(() =>
				{
					using (var pub = ctx.Socket(SocketType.PUB))
					{
						pub.Bind("pgm://192.168.0.110:8082");
						Thread.Sleep(300);
						using (var f = File.Open("queue", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
						{
							var buffer = new byte[16];

							while (!t.Token.IsCancellationRequested)
							{
								while (publisherIdx < unmarshallerIdx)
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
						eh.Connect("pgm://192.168.0.110:8082");
						eh.Subscribe(new byte[0]);
						while (!t.Token.IsCancellationRequested)
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

			var watch = Stopwatch.StartNew();

			int handlerStart = handlerIdx;
			int clientStart = cliendIdx;
			var storeStart = unmarshallerIdx;
			for (int i = 0; i < int.MaxValue; i++)
			{
				Thread.Sleep(1000);
				var totalSeconds = watch.Elapsed.TotalSeconds;
				var storeMps = Math.Round((unmarshallerIdx - storeStart) / totalSeconds);

				var handlerMps = Math.Round((handlerIdx - handlerStart) / totalSeconds);
				var clientMps = Math.Round((cliendIdx - clientStart) / totalSeconds);

				Console.WriteLine("{0}           {1}            {2} (msg per second)", clientMps, storeMps, handlerMps);
				if (i > 11)
					break;
			}

			Console.ReadLine();
			while (true) {}
		}
	}
}