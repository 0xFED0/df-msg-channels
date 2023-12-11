using Nito.Disposables;
using System.Threading.Tasks.Dataflow;

namespace Channels
{
    using MsgSrcBlock = ISourceBlock<Message>;
    using MsgTgtBlock = ITargetBlock<Message>;
    using MsgPropagator = IPropagatorBlock<Message, Message>;
    using MsgChannel = IPropagatorBlock<Message, Message>;

    public class Channel
    {
        public MsgChannel IO { get => _io; }
        public MsgSrcBlock? Source { get => _source; set => SetSource(value); }
        public MsgTgtBlock? Target { get => _target; set => SetTarget(value); }
        public bool IsSourceConnected { get => _source_link != null; }
        public bool IsTargetConnected { get => _target_link != null; }
        public bool IsConnected { get => IsSourceConnected || IsTargetConnected; }

        public void Disconnect() { SetSource(null); SetTarget(null); }

        public delegate void BlockChanged(Channel channel, IDataflowBlock source);
        public BlockChanged? SourceConnected;
        public BlockChanged? TargetConnected;
        public BlockChanged? SourceDisconnected;
        public BlockChanged? TargetDisconnected;

        public Channel(
                MsgSrcBlock? source = null, MsgTgtBlock? target = null,
                DataflowBlockOptions? inOptions = null, DataflowBlockOptions? outOptions = null
            )
        {
            _in = new BroadcastBlock<Message>(m => m, inOptions ?? InBlockOptions);
            _out = new BufferBlock<Message>(outOptions ?? OutBlockOptions);
            _io = DataflowBlock.Encapsulate(_out, _in);
            SetSource(source); SetTarget(target);
        }

        public virtual Channel Proxy(DataflowBlockOptions? inOptions = null, DataflowBlockOptions? outOptions = null)
            => new Channel(_io, _io, inOptions, outOptions);

        public virtual void SetSource(MsgSrcBlock? source, DataflowLinkOptions? options = null)
        {
            if (source == _source) return;
            _source_link?.Dispose();
            _source_link = LinkTilCompletion(source, _in, options ?? SourceLinkOptions);
            if (_source_link != null && source != null)
            {
                _source = source;
                _source_link.Add(OnDisposeSourceLink);
                SourceConnected?.Invoke(this, source);
            }
        }

        public virtual void SetTarget(MsgTgtBlock? target, DataflowLinkOptions? options = null)
        {
            if (target == _target) return;
            _target_link?.Dispose();
            _target_link = LinkTilCompletion(_out, target, options ?? TargetLinkOptions);
            if (_target_link != null && target != null)
            {
                _target = target;
                _target_link?.Add(OnDisposeTargetLink);
                TargetConnected?.Invoke(this, target);
            }
        }

        private void OnDisposeSourceLink()
        {
            _source_link = null;
            if (_source != null)
                SourceDisconnected?.Invoke(this, _source);
            _source = null;
        }

        private void OnDisposeTargetLink()
        {
            _target_link = null;
            if (_target != null)
                TargetDisconnected?.Invoke(this, _target);
            _target = null;
        }

        private MsgChannel _io;
        private MsgPropagator _in;
        private MsgPropagator _out;

        private MsgSrcBlock? _source;
        private Disposable? _source_link;
        private MsgTgtBlock? _target;
        private Disposable? _target_link;

        internal static readonly DataflowLinkOptions SourceLinkOptions
            = new DataflowLinkOptions() { PropagateCompletion = false };
        internal static readonly DataflowLinkOptions TargetLinkOptions
            = new DataflowLinkOptions() { PropagateCompletion = false };
        internal static readonly DataflowBlockOptions InBlockOptions
            = new DataflowBlockOptions() { EnsureOrdered = true };
        internal static readonly DataflowBlockOptions OutBlockOptions
            = new DataflowBlockOptions() { EnsureOrdered = true };

        private static Disposable? LinkTilCompletion(MsgSrcBlock? src, MsgTgtBlock? tgt, DataflowLinkOptions options)
        {
            if (src == null || tgt == null) return null;
            var result = new Disposable(null);
            var link = src.LinkTo(tgt, options);
            var cancel = new CancellationTokenSource();
            Task.WhenAny(src.Completion, tgt.Completion)
                .ContinueWith(t =>
                {
                    cancel?.Dispose(); cancel = null;
                    result.Dispose();
                },
                    cancel.Token);
            result.Add(() =>
            {
                cancel?.Cancel(); cancel?.Dispose(); cancel = null;
                link?.Dispose(); link = null;
            });
            return result;
        }
    }

    public class Message
    {
        public string Type = "";
        public object Data = EmptyData;
        public EndPoint? Sender;

        public static readonly object EmptyData = new object();

        public virtual bool IsValid() => Type != "";

        public virtual bool FromBytes(byte[] packet) => false;
        public virtual byte[] ToBytes() { return new byte[0]; }
        public virtual void WriteTo(Stream stream)
        {
            if (!IsValid()) return;
            var buffer = ToBytes();
            stream.Write(buffer);
        }
        public virtual bool ReadFrom(Stream stream) => false;

        public virtual void PutSender(EndPoint endPoint)
        {
            Sender = endPoint.CloneOrigin(Sender);
        }

        public static Message ReadDelegate<MsgClass>(Stream stream)
            where MsgClass : Message, new()
        {
            var msg = new MsgClass();
            if (!msg.ReadFrom(stream))
                throw new IOException("Couldn't read Message from stream");
            return msg;
        }
        public static void WriteDelegate(Stream stream, Message msg) => msg.WriteTo(stream);

        public static Message BytesToMessage<MsgClass>(byte[] packet)
            where MsgClass : Message, new()
        {
            var msg = new MsgClass();
            msg.FromBytes(packet);
            return msg;
        }
        public static byte[] MessageToBytes(Message msg) => msg.ToBytes();
    }

    public class EndPoint : ICloneable
    {
        public string Name = "";
        public EndPoint? From;
        public virtual bool Match(EndPoint? other)
            => other != null && (this == other || Name == other.Name);
        public bool SenderMatch(Message msg) => Match(msg.Sender);
        public bool FullSenderMatch(Message msg) => Match(msg.Sender) && (From?.FullSenderMatch(msg) ?? true);

        public virtual object Clone() => MemberwiseClone();

        public EndPoint? GetPrev() => From;
        public EndPoint? GetOrigin()
            => ChainSearch(ep => ep.From == null, false);
        public EndPoint? ChainSearch(Func<EndPoint, bool> predicate, bool fromThis = true)
        {
            var from = fromThis ? this : this.From;
            while (from != null)
            {
                if (predicate(from))
                    return from;
                from = from.From;
            }
            return null;
        }

        public EndPoint WithPrev(EndPoint? from)
        {
            if (from == null)
                return this;
            var first = from.GetOrigin();
            if (first != null)
                first.From = From;
            From = from;
            return this;
        }

        public EndPoint WithOrigin(EndPoint? from)
        {
            if (from == null)
                return this;
            var first = GetOrigin() ?? this;
            first.From = from;
            return this;
        }

        public EndPoint CloneOrigin(EndPoint? from)
            => ((EndPoint)Clone()).WithOrigin(from);

        public EndPoint ClonePrev(EndPoint? from)
            => ((EndPoint)Clone()).WithPrev(from);
    }

}
