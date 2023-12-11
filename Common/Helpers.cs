using System.IO;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Channels
{
    using MsgIO = IPropagatorBlock<Message, Message>;
    using MsgPropagator = IPropagatorBlock<Message, Message>;
    using MsgTransform = TransformBlock<Message, Message>;

    using MsgPredicate = Predicate<Message>;
    using MsgToMsg = Func<Message, Message>;
    using OnMsg = Action<Message>;
    using MsgToMsgAsync = Func<Message, Task<Message>>;
    using OnMsgAsync = Func<Message, Task>;


    public static class IO {
        public static Channel ByStream<MsgClass>(Stream iostream) where MsgClass : Message, new()
            => ByStream<MsgClass>(iostream, iostream);
        public static Channel ByStream<MsgClass>(Stream istream, Stream ostream) where MsgClass : Message, new()
            => ByStream(istream, Message.ReadDelegate<MsgClass>, ostream, Message.WriteDelegate);
        public static Channel ByStream(Stream? istream, Func<Stream, Message>? read, Stream? ostream, Action<Stream, Message>? write = null)
        {
            var src = (istream != null && read != null) ? MsgSource.FromStream(istream, read) : null;
            var tgt = (ostream != null) ? MsgTarget.ToStream(ostream, write ?? Message.WriteDelegate) : null;
            return new Channel(src, tgt);
        }
        public static Channel ByStream(Stream? istream, Func<Stream, Task<Message>>? read, Stream? ostream, Func<Stream, Message, Task>? write)
        {
            var src = (istream != null && read != null) ? MsgSource.FromStream(istream, read) : null;
            var tgt = (ostream != null && write != null) ? MsgTarget.ToStream(ostream, write) : null;
            return new Channel(src, tgt);
        }

        public static MsgIO StreamBlock<MsgClass>(Stream iostream) where MsgClass : Message, new()
            => StreamBlock<MsgClass>(iostream, iostream);
        public static MsgIO StreamBlock<MsgClass>(Stream istream, Stream ostream) where MsgClass : Message, new()
            => StreamBlock(istream, Message.ReadDelegate<MsgClass>, ostream);
        public static MsgIO StreamBlock(Stream istream, Func<Stream, Task<Message>> read, Stream ostream, Func<Stream, Message, Task> write)
        {
            var src = MsgSource.FromStream(istream, read);
            var tgt = MsgTarget.ToStream(ostream, write);
            if (src == null) throw new ArgumentException("Bad input stream " + nameof(istream));
            if (tgt == null) throw new ArgumentException("Bad output stream " + nameof(ostream));
            return DataflowBlock.Encapsulate(tgt, src);
        }
        public static MsgIO StreamBlock(Stream istream, Func<Stream, Message> read, Stream ostream, Action<Stream, Message>? write = null)
        {
            var src = MsgSource.FromStream(istream, read);
            var tgt = MsgTarget.ToStream(ostream, write ?? Message.WriteDelegate);
            if (src == null) throw new ArgumentException("Bad input stream " + nameof(istream));
            if (tgt == null) throw new ArgumentException("Bad output stream " + nameof(ostream));
            return DataflowBlock.Encapsulate(tgt, src);
        }
    }

    public static class ReceiveMsg
    {
        public static Task<Message?> WaitMessage(this ISourceBlock<Message> source, TimeSpan timeout)
            => source.WaitPacket(timeout);
        public static Task<Message?> WaitMessage(this ISourceBlock<Message> source, MsgPredicate predicate, TimeSpan timeout)
            => source.WaitPacket(predicate, timeout);

        public static IDisposable OnMessage(this ISourceBlock<Message> source, OnMsg callback, MsgPredicate? predicate = null)
            => source.OnPacket(callback, predicate);
        public static IDisposable OnMessage(this ISourceBlock<Message> source, OnMsgAsync callback, MsgPredicate? predicate = null)
            => source.OnPacket(callback, predicate);

        public static IDisposable OnMessage(this ISourceBlock<Message> source, string MsgType, OnMsg callback)
            => source.OnMessage(callback, msg => msg.Type == MsgType);
        public static IDisposable OnMessage(this ISourceBlock<Message> source, string MsgType, OnMsgAsync callback)
            => source.OnMessage(callback, msg => msg.Type == MsgType);
        public static IDisposable OnMessage(this ISourceBlock<Message> source, Regex MsgType, OnMsg callback)
            => source.OnMessage(callback, msg => MsgType.IsMatch(msg.Type));
        public static IDisposable OnMessage(this ISourceBlock<Message> source, Regex MsgType, OnMsgAsync callback)
            => source.OnMessage(callback, msg => MsgType.IsMatch(msg.Type));

        public static IDisposable OnMessageMatch(this ISourceBlock<Message> source, string MsgTypePattern, OnMsg callback)
            => source.OnMessage(new Regex(MsgTypePattern), callback);
        public static IDisposable OnMessageMatch(this ISourceBlock<Message> source, string MsgTypePattern, OnMsgAsync callback)
            => source.OnMessage(new Regex(MsgTypePattern), callback);
    }

    public static class MsgTarget
    {
        public static ITargetBlock<Message> ToNull() => DataflowBlock.NullTarget<Message>();

        public static ITargetBlock<Message>? ToStream(Stream stream)
            => AnyTarget.ToStream<Message>(stream, Message.WriteDelegate);
        public static ITargetBlock<Message>? ToStream(Stream stream, Func<Stream, Message, Task> writeAsync)
            => AnyTarget.ToStream(stream, writeAsync);
        public static ITargetBlock<Message>? ToStream(Stream stream, Action<Stream, Message> writeSync)
            => AnyTarget.ToStream(stream, writeSync);
        public static ITargetBlock<byte[]>? BufferToStream(Stream stream)
            => AnyTarget.ToStream<byte[]>(stream, (stream, data) => stream.Write(data));

        public static ITargetBlock<Message> WithFilter(this ITargetBlock<Message> target, MsgPredicate predicate)
            => AnyTarget.WithFilter(target, predicate);

        public static ITargetBlock<Message> WithTransformer(this ITargetBlock<Message> target, MsgToMsg transformer)
            => AnyTarget.WithConverter(target, transformer);

        public static ITargetBlock<Message> WithTransformer(this ITargetBlock<Message> target, MsgToMsgAsync transformer)
            => AnyTarget.WithConverter(target, transformer);

        public static ITargetBlock<Message> WithConverterTo<T>(this ITargetBlock<T> target, Func<Message, T> transformer)
            => AnyTarget.WithConverter(target, transformer);
        public static ITargetBlock<Message> WithConverterTo<T>(this ITargetBlock<T> target, Func<Message, Task<T>> transformer)
            => AnyTarget.WithConverter(target, transformer);

        public static ITargetBlock<T> WithConverterFrom<T>(this ITargetBlock<Message> target, Func<T, Message> transformer)
            => AnyTarget.WithConverter(target, transformer);
        public static ITargetBlock<T> WithConverterFrom<T>(this ITargetBlock<Message> target, Func<T, Task<Message>> transformer)
            => AnyTarget.WithConverter(target, transformer);
    }

    public static class MsgSource
    {
        public static ISourceBlock<Message>? FromStream<MsgClass>(Stream stream) where MsgClass : Message, new()
            => FromStream(stream, Message.ReadDelegate<MsgClass>);
        public static ISourceBlock<Message>? FromStream(Stream stream, Func<Stream, Task<Message>> readAsync)
            => AnySource.FromStream(stream, readAsync);
        public static ISourceBlock<Message>? FromStream(Stream stream, Func<Stream, Message> readSync)
            => AnySource.FromStream(stream, readSync);
        public static ISourceBlock<byte[]>? FromStream(Stream stream, Func<Stream, Task<byte[]>> readAsync)
            => AnySource.FromStream(stream, readAsync);
        public static ISourceBlock<byte[]>? FromStream(Stream stream, Func<Stream, byte[]> readSync)
            => AnySource.FromStream(stream, readSync);
        public static ISourceBlock<Message> FromEnumerator(IAsyncEnumerable<Message> messages)
            => AnySource.FromEnumerator(messages);
        public static ISourceBlock<Message> FromEnumerator(IEnumerable<Message> messages)
            => AnySource.FromEnumerator(messages);

        public static IAsyncEnumerable<Message> AsEnumerator(this IReceivableSourceBlock<Message> target)
            => AnySource.AsEnumerator(target);

        public static ISourceBlock<Message> Filtered(this ISourceBlock<Message> source, MsgPredicate predicate)
            => AnySource.Filter(source, predicate);

        public static ISourceBlock<Message> Transformed(this ISourceBlock<Message> source, MsgToMsg transformer)
            => AnySource.Converter(source, transformer);

        public static ISourceBlock<Message> Transformed(this ISourceBlock<Message> source, MsgToMsgAsync transformer)
            => AnySource.Converter(source, transformer);

        public static ISourceBlock<Message> ConvertedToMsg<T>(this ISourceBlock<T> source, Func<T, Message> transformer)
            => AnySource.Converter(source, transformer);
        public static ISourceBlock<Message> ConvertedToMsg<T>(this ISourceBlock<T> source, Func<T, Task<Message>> transformer)
            => AnySource.Converter(source, transformer);

        public static ISourceBlock<T> ConvertedFromMsg<T>(this ISourceBlock<Message> source, Func<Message, T> transformer)
            => AnySource.Converter(source, transformer);
        public static ISourceBlock<T> ConvertedFromMsg<T>(this ISourceBlock<Message> source, Func<Message, Task<T>> transformer)
            => AnySource.Converter(source, transformer);
    }

    public static class ReceiveAny
    {
        public static Task<Packet?> WaitPacket<Packet>(this ISourceBlock<Packet> source, TimeSpan timeout)
            => WaitPacket(source, null, new CancellationTokenSource(timeout).Token);
        public static Task<Packet?> WaitPacket<Packet>(this ISourceBlock<Packet> source, Predicate<Packet> predicate, TimeSpan timeout)
            => WaitPacket(source, predicate, new CancellationTokenSource(timeout).Token);
        public static Task<Packet?> WaitPacket<Packet>(this ISourceBlock<Packet> source, CancellationToken cancel)
            => WaitPacket(source, null, cancel);
        public static Task<Packet?> WaitPacket<Packet>(this ISourceBlock<Packet> source, Predicate<Packet>? predicate, CancellationToken? cancel = null)
        {
            var promise = new TaskCompletionSource<Packet?>();
            if (cancel.HasValue)
                cancel?.Register(() => promise?.TrySetCanceled());

            var link = source.OnPacket(p => promise?.TrySetResult(p), predicate);
            return promise.Task.ContinueWith(t =>
            {
                promise = null;
                link.Dispose();
                return t.IsCompletedSuccessfully ? t.Result : default;
            });
        }

        public static IDisposable OnPacket<Packet>(this ISourceBlock<Packet> source, Action<Packet> callback, Predicate<Packet>? predicate = null)
        {
            var block = new ActionBlock<Packet>(callback, ActionOptions);
            return predicate != null ? source.LinkTo(block, PredicateReceiveOptions, predicate) : source.LinkTo(block, ReceiveOptions);
        }
        public static IDisposable OnPacket<Packet>(this ISourceBlock<Packet> source, Func<Packet, Task> callback, Predicate<Packet>? predicate = null)
        {
            var block = new ActionBlock<Packet>(callback, ActionOptions);
            return predicate != null ? source.LinkTo(block, PredicateReceiveOptions, predicate) : source.LinkTo(block, ReceiveOptions);
        }

        internal static ExecutionDataflowBlockOptions ActionOptions = new ()
        {
            EnsureOrdered = true,
            SingleProducerConstrained = true,
        };

        internal static DataflowLinkOptions ReceiveOptions = new() { Append = true };

        internal static DataflowLinkOptions PredicateReceiveOptions = new() { Append = false };
    }

    public static class AnyTarget
    {
        public static ITargetBlock<Packet>? ToStream<Packet>(Stream stream, Func<Stream, Packet, Task> writeAsync)
        {
            return ToStream<Packet>(stream, (s, d) => writeAsync(s, d).Wait());
        }
        public static ITargetBlock<Packet>? ToStream<Packet>(Stream stream, Action<Stream, Packet> writeSync)
        {
            if (!stream.CanWrite)
                return null;
            ActionBlock<Packet>? block = null;
            block = new ActionBlock<Packet>((data) =>
            {
                if (!Utils.TestWriteStream(stream))
                    block?.Complete(); // seems stream closed, so mark block as completed
                try
                {
                    if (stream.CanWrite)
                        writeSync(stream, data);
                }
                finally
                {
                    try { stream.Flush(); } catch { };
                }
            }, StreamActionOptions);
            return block;
        }

        public static ITargetBlock<Packet> WithFilter<Packet>(ITargetBlock<Packet> target, Predicate<Packet> predicate)
        {
            var input = new BufferBlock<Packet>();
            input.LinkTo(target, predicate);
            return input;
        }

        public static ITargetBlock<Packet> WithTransformer<Packet>(ITargetBlock<Packet> target, Func<Packet, Packet> transformer)
            => WithConverter(target, transformer);

        public static ITargetBlock<Packet> WithTransformer<Packet>(ITargetBlock<Packet> target, Func<Packet, Task<Packet>> transformer)
            => WithConverter(target, transformer);

        public static ITargetBlock<I> WithConverter<I,O>(ITargetBlock<O> target, Func<I, O> transformer)
        {
            var input = new TransformBlock<I, O>(transformer);
            input.LinkTo(target);
            return input;
        }

        public static ITargetBlock<I> WithConverter<I, O>(ITargetBlock<O> target, Func<I, Task<O>> transformer)
        {
            var input = new TransformBlock<I, O>(transformer);
            input.LinkTo(target);
            return input;
        }

        public static Task<bool> PostOrWait<Packet>(this ITargetBlock<Packet> target, Packet data)
        {
            if (!target.Post(data))
                return target.SendAsync(data);
            return Task.FromResult(true);
        }

        internal static ExecutionDataflowBlockOptions StreamActionOptions
            = new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1,
                MaxMessagesPerTask = 1,
                EnsureOrdered = true,
            };
    }

    public static class AnySource
    {
        public static ISourceBlock<Packet>? FromStream<Packet>(Stream istream, Func<Stream, Task<Packet>> readAsync, Func<Packet, Packet>? cloning = null)
            => FromStream(istream, s => readAsync(s).Result, cloning);
        public static ISourceBlock<Packet>? FromStream<Packet>(Stream istream, Func<Stream, Packet> readSync, Func<Packet, Packet>? cloning = null)
        {
            if (!istream.CanRead)
                return null;
            var source = AnyBlocks.Echo(cloning);
            var task = Utils.StreamLoopTask(istream, (istream) =>
            {
                if (!istream.CanRead || !Utils.TestReadStream(istream))
                    return false; // seems stream is invalid or closed. break the loop
                Packet packet;
                try
                {
                    packet = readSync(istream);
                } catch
                { // couldn't obtain/parse packet
                    return true; // just skip
                }
                return source.PostOrWait(packet).Result; // result break the loop if block is completed
            });
            Utils.CompleteWhenTaskEnds(source, task);
            return source;
        }

        public static ISourceBlock<Packet> FromEnumerator<Packet>(IEnumerable<Packet> packets, Func<Packet, Packet>? cloning = null)
        {
            var source = AnyBlocks.Echo(cloning);
            var task = packets.ForEachAsync((Packet p) => source.PostOrWait(p).Result);
            Utils.CompleteWhenTaskEnds(source, task);
            return source;
        }
        public static ISourceBlock<Packet> FromEnumerator<Packet>(IAsyncEnumerable<Packet> packets, Func<Packet, Packet>? cloning = null)
        {
            var source = AnyBlocks.Echo(cloning);
            var task = packets.ForEachAsync((Packet p) => source.PostOrWait(p));
            Utils.CompleteWhenTaskEnds(source, task);
            return source;
        }

        public static async IAsyncEnumerable<Packet> AsEnumerator<Packet>(ISourceBlock<Packet> target)
        {
            while (await target.OutputAvailableAsync())
                yield return (await target.ReceiveAsync());
        }

        public static ISourceBlock<Packet> Filter<Packet>(ISourceBlock<Packet> source, Predicate<Packet> predicate, Func<Packet, Packet>? cloning = null)
        {
            var output = AnyBlocks.Echo(cloning);
            source.LinkTo(output, ReceiveAny.PredicateReceiveOptions, predicate);
            return output;
        }

        public static ISourceBlock<Packet> Transformer<Packet>(ISourceBlock<Packet> source, Func<Packet, Packet> transformer)
            => Converter(source, transformer);

        public static ISourceBlock<Packet> Transformer<Packet>(ISourceBlock<Packet> source, Func<Packet, Task<Packet>> transformer)
            => Converter(source, transformer);

        public static ISourceBlock<O> Converter<I, O>(ISourceBlock<I> source, Func<I, O> transformer)
        {
            var output = new TransformBlock<I, O>(transformer);
            source.LinkTo(output);
            return output;
        }

        public static ISourceBlock<O> Converter<I, O>(ISourceBlock<I> source, Func<I, Task<O>> transformer)
        {
            var output = new TransformBlock<I, O>(transformer);
            source.LinkTo(output);
            return output;
        }
    }

    public static class Utils
    {
        public static Task CompleteWhenTaskEnds(IDataflowBlock block, Task task)
        {
            return task.ContinueWith((Task t) =>
            {
                if (t.IsFaulted && t.Exception != null)
                    block.Fault(t.Exception);
                else
                    block.Complete();
            });
        }

        public static async Task CloseStreamWhenNoIO(Stream stream, int checkIntervalMS = 1000)
        {
            try
            {
                while(true) {
                    await Task.Delay(checkIntervalMS);
                    if (stream.CanRead)
                        await stream.ReadAsync(empty, 0, 0);
                    if (stream.CanWrite)
                        await stream.WriteAsync(empty, 0, 0);
                }
            } catch { }
            stream.Close();
        }

        public static Task StreamLoopTask(Stream stream, Func<Stream, bool> action, bool closeAtFinish = true)
            => Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        if (!action(stream))
                            break;
                    }
                    catch {
                        break;
                    }
                }
                stream.Close();
            });

        private static byte[] empty = new byte[0];
        public static bool TestReadStream(Stream stream)
        { try { stream.Read(empty, 0, 0); } catch { return false; } return true; }
        public static bool TestWriteStream(Stream stream)
        { try { stream.Write(empty, 0, 0); } catch { return false; } return true; }

        public static Task ForEachAsync<T>(this IAsyncEnumerable<T> enumerable, Func<T, Task<bool>> action)
            => Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in enumerable)
                    {
                        if (!await action(item))
                            break;
                    }
                } catch { }
            });

        public static Task ForEachAsync<T>(this IEnumerable<T> enumerable, Func<T, bool> action)
            => Task.Factory.StartNew(() =>
            {
                try
                {
                    foreach (var item in enumerable)
                        if (!action(item))
                            break;
                }
                catch { }
            });

        public static async IAsyncEnumerable<T> AsAsync<T>(this IEnumerable<T> sync)
        {
            var enumerator = sync.GetEnumerator();
            while (await Task.Run(enumerator.MoveNext))
            {
                yield return enumerator.Current;
            }
        }

        public static IEnumerable<T> AsSync<T>(this IAsyncEnumerable<T> async)
        {
            var enumerator = async.GetAsyncEnumerator();
            while (enumerator.MoveNextAsync().Result)
            {
                yield return enumerator.Current;
            }
        }
    }
}
