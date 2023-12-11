using System.Formats.Asn1;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Channels.Connectors
{
    using BaseConnection = Connector.Connection;

    public static class TcpConnector
    {
        public static ISourceBlock<BaseConnection> AcceptFrom(IPEndPoint endPoint, int timeoutMS = 0)
            => Connector.Source(TcpAcceptEnumerator(endPoint, timeoutMS));

        public static ISourceBlock<BaseConnection> ReconnectTo(IEnumerable<IPEndPoint> endPoints, TimeSpan interval,
            int ioTimeoutMS = 0, TimeSpan? connectionTimeout = null)
            => Connector.Source(TcpReconnectEnumerator(endPoints, interval, ioTimeoutMS, connectionTimeout));

        public class Connection : BaseConnection
        {
            [NonSerialized()] public readonly TcpClient Client;

            public Connection(TcpClient client)
            {
                Client = client;
                var stream = client.GetStream();
                RemoteAddr = new EndPoint() { Stream = stream, IpAndPort = client.Client.RemoteEndPoint?.ToString() };
                LocalAddr = new EndPoint() { Stream = stream, IpAndPort = client.Client.LocalEndPoint?.ToString() };
            }

            public override void Close()
            {
                if (!IsOpen) return;
                base.Close();
                Client.Close();
            }
        }

        public class EndPoint : StreamEndPoint
        {
            public string? IpAndPort;

            public override bool Match(Channels.EndPoint? other)
            {
                if (other == null || this.IpAndPort == null)
                    return false;
                if (this == other)
                    return true;
                return IpAndPort.Equals((other as EndPoint)?.IpAndPort);
            }
        }

        private static async IAsyncEnumerable<BaseConnection> TcpAcceptEnumerator(IPEndPoint endPoint, int timeoutMS = 0)
        {
            var listener = new TcpListener(endPoint);
            listener.Start();

            using(listener.Server)
            {
                while(true)
                {
                    TcpClient client;
                    try
                    {
                        client = await listener.AcceptTcpClientAsync();
                        client.ReceiveTimeout = timeoutMS;
                        client.SendTimeout = timeoutMS;
                    }
                    catch { break; }

                    yield return new Connection(client);
                }
            }
        }

        private static TimeSpan DefConnectionTimeout = TimeSpan.FromSeconds(5);
        private static async IAsyncEnumerable<BaseConnection> TcpReconnectEnumerator(
            IEnumerable<IPEndPoint> endPoints, TimeSpan interval,
            int ioTimeoutMS = 0, TimeSpan? connectionTimeout = null)
        {
            var connections = endPoints.Select(addr => {
                return new KeyValuePair<IPEndPoint, Connection?>(addr, null);
            }).ToArray();

            while (true)
            {
                for(int i = 0; i < connections.Length; i++)
                {
                    var pair = connections[i];
                    if (pair.Value?.IsOpen ?? false) continue;

                    var client = new TcpClient();
                    try
                    {
                        var timeout = new CancellationTokenSource();
                        timeout.CancelAfter(connectionTimeout ?? DefConnectionTimeout);
                        await client.ConnectAsync(pair.Key, timeout.Token);
                        client.ReceiveTimeout = ioTimeoutMS;
                        client.SendTimeout = ioTimeoutMS;
                        client.ReceiveBufferSize = 32 * 1024;
                        client.SendBufferSize = 32 * 1024;
                    }
                    catch { client.Dispose(); }

                    if (client.Connected)
                    {
                        var addr = pair.Key;
                        Connection? connection = new Connection(client);
                        connections[i] = new KeyValuePair<IPEndPoint, Connection?>(addr, connection);
                        yield return connection;
                    } else
                    {
                        client.Dispose();
                    }
                }
                await Task.Delay(interval);
            }
        }
    }
}
