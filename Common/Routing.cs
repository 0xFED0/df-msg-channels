using Nito.Disposables;
using System.Net.Sockets;
using System.Threading.Tasks.Dataflow;

namespace Channels
{
    using MsgSrcBlock = ISourceBlock<Message>;
    using MsgTgtBlock = ITargetBlock<Message>;
    using MsgPropagator = IPropagatorBlock<Message, Message>;
    using MsgIO = IPropagatorBlock<Message, Message>;

    using MsgPredicate = Predicate<Message>;
    using MsgToMsg = Func<Message, Message>;
    using OnMsg = Action<Message>;
    using MsgToMsgAsync = Func<Message, Task<Message>>;
    using OnMsgAsync = Func<Message, Task>;

    using BaseConnection = Connector.Connection;

    public static class Connector
    {
        public class Connection
        {
            [NonSerialized()] public MsgSrcBlock? Input;
            [NonSerialized()] public MsgTgtBlock? Output;

            public EndPoint? RemoteAddr;
            public EndPoint? LocalAddr;
            public bool IsRejected;

            public bool IsOpen
            {
                get => !(Input?.Completion.IsCompleted ?? false) || !(Output?.Completion.IsCompleted ?? false);
            }

            public virtual void Close()
            {
                Input?.Complete();
                Output?.Complete();
            }
        }

        public delegate bool ReconnectPredicate(Connection connection);

        public static ISourceBlock<Connection> Source(IAsyncEnumerable<Connection> AcceptGenerator)
            => AnySource.FromEnumerator(AcceptGenerator);

        public static ISourceBlock<Connection> ConfiguringConnection(this ISourceBlock<Connection> source, Func<Connection, Task<Connection>> func)
            => AnySource.Converter(source, func);

        /// <summary>
        /// Make <c>Input</c> and <c>Output</c> blocks
        /// </summary>
        public static ISourceBlock<Connection> OpenningIO(this ISourceBlock<Connection> source,
            Func<Connection, Task<MsgSrcBlock?>>? openIn, Func<Connection, Task<MsgTgtBlock?>>? openOut)
            => AnySource.Converter(source, async (connection) =>
            {
                if (openIn != null)
                    connection.Input = await openIn(connection);
                if (openOut != null)
                    connection.Output = await openOut(connection);

                var closeAction = (Task t) => connection.Close();
                connection.Input?.Completion.ContinueWith(closeAction);
                connection.Output?.Completion.ContinueWith(closeAction);

                return connection;
            });

        /// <summary>
        /// <c>Handshake</c> detects endpoints.
        /// </summary>
        public static ISourceBlock<Connection> Handshake(this ISourceBlock<Connection> source, Func<Connection, Task<Connection>> func)
            => AnySource.Converter(source, func);

        public static ReliefBlock<Connection> Linker(this ISourceBlock<Connection> source, Func<Connection, Task> onRejected)
            => source.Linker(new ActionBlock<Connection>(onRejected));
        public static ReliefBlock<Connection> Linker(this ISourceBlock<Connection> source, ITargetBlock<Connection>? rejectedTo = null)
        {
            var linker = Linker(rejectedTo);
            source.LinkTo(linker);
            return linker;
        }
        public static ReliefBlock<Connection> Linker(ITargetBlock<Connection>? rejectedTo = null)
        {
            var linker = AnyBlocks.DropRejected(rejectedTo ?? new ActionBlock<Connection>(c => c.Close()));
            linker.LinkTo(new ActionBlock<Connection>(c => { linker.DropTarget.Post(c); }), c => c.IsRejected);
            return linker;
        }

        public static IDisposable LinkByAddr(this ReliefBlock<Connection> source, Channel channel, EndPoint? remote, EndPoint? local = null)
            => source.LinkChannel(channel, c => (remote?.Match(c.RemoteAddr) ?? true) && (local?.Match(c.LocalAddr) ?? true));
        public static IDisposable LinkChannel(this ReliefBlock<Connection> source, Channel channel, ReconnectPredicate predicate)
        {
            var holder = new Disposable(channel.Disconnect);
            var link = source.OnPacket((connection) =>
                {
                    channel.Source = connection.Input;
                    channel.Target = connection.Output;
                },
                (connection) => connection.IsOpen && !channel.IsConnected && predicate(connection)
            );
            holder.Add(link.Dispose);
            return holder;
        }

    }

    static class StreamBased
    {
        public static ISourceBlock<BaseConnection> StreamOpenningIO<MsgClass>(this ISourceBlock<BaseConnection> source)
            where MsgClass : Message, new()
            => source.OpenningIO(OpenInputByStream<MsgClass>, OpenOutputByStream);

        public static Task<MsgSrcBlock?> OpenInputByStream<MsgClass>(BaseConnection connection)
            where MsgClass : Message, new()
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgSource.FromStream<MsgClass>(stream)) as MsgSrcBlock);
        public static Task<MsgSrcBlock?> OpenInputByStream(BaseConnection connection, Func<Stream, Task<Message>> readAsync)
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgSource.FromStream(stream, readAsync)) as MsgSrcBlock);
        public static Task<MsgSrcBlock?> OpenInputByStream(BaseConnection connection, Func<Stream, Message> readSync)
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgSource.FromStream(stream, readSync)) as MsgSrcBlock);

        public static Task<MsgTgtBlock?> OpenOutputByStream(BaseConnection connection)
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgTarget.ToStream(stream)) as MsgTgtBlock);
        public static Task<MsgTgtBlock?> OpenOutputByStream(BaseConnection connection, Func<Stream, Message, Task> writeAsync)
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgTarget.ToStream(stream, writeAsync)) as MsgTgtBlock);
        public static Task<MsgTgtBlock?> OpenOutputByStream(BaseConnection connection, Action<Stream, Message> writeSync)
            => Task.FromResult(BlockByStreamConnection(connection, stream => MsgTarget.ToStream(stream, writeSync)) as MsgTgtBlock);

        private static IDataflowBlock? BlockByStreamConnection(BaseConnection connection, Func<Stream, IDataflowBlock?> openFunc)
        {
            var stream = connection.GetStream();
            if (stream == null)
                return null;
            var block = openFunc(stream);
            block?.Completion.ContinueWith(t => stream.Close());
            Utils.CloseStreamWhenNoIO(stream).ContinueWith(t => block?.Complete());
            return block;
        }

        public static Stream? GetStream(this BaseConnection connection)
        {
            var sEp = connection.RemoteAddr?.ChainSearch(e => e is StreamEndPoint) as StreamEndPoint;
            if (sEp == null)
                return null;
            return sEp.Stream;
        }

        public static void SetStream(this BaseConnection connection, Stream? stream)
        {
            var sEp = connection.RemoteAddr?.ChainSearch(e => e is StreamEndPoint) as StreamEndPoint;
            if (sEp != null)
                sEp.Stream = stream;
        }
    }

    public static class Address
    {
        public static MsgSrcBlock Router(MsgSrcBlock source)
        {
            var router = AnyBlocks.Switch<Message>();
            source.LinkTo(router);
            return router;
        }

        public static IDisposable OnMessageMatchSender(this MsgSrcBlock source, EndPoint sender, OnMsg callback)
            => source.OnMessage(callback, sender.SenderMatch);
        public static IDisposable OnMessageMatchSender(this MsgSrcBlock source, EndPoint sender, OnMsgAsync callback)
            => source.OnMessage(callback, sender.SenderMatch);
        public static IDisposable OnMessageMatchFullSender(this MsgSrcBlock source, EndPoint sender, OnMsg callback)
            => source.OnMessage(callback, sender.FullSenderMatch);
        public static IDisposable OnMessageMatchFullSender(this MsgSrcBlock source, EndPoint sender, OnMsgAsync callback)
            => source.OnMessage(callback, sender.FullSenderMatch);

        public static MsgSrcBlock SenderFiltered(this MsgSrcBlock source, EndPoint sender)
            => source.Filtered(sender.SenderMatch);
        public static MsgSrcBlock FullSenderFiltered(this MsgSrcBlock source, EndPoint sender)
            => source.Filtered(sender.FullSenderMatch);

        public static MsgTgtBlock WithSender(this MsgTgtBlock target, EndPoint sender)
            => target.WithTransformer(msg => { msg.PutSender(sender); return msg; });
    }

    public class StreamEndPoint : EndPoint
    {
        [NonSerialized()] public Stream? Stream;

        public override bool Match(EndPoint? other)
        {
            if (other == null || this.Stream == null)
                return false;
            if (this == other)
                return true;
            return Stream.Equals((other as StreamEndPoint)?.Stream);
        }
    }
}
