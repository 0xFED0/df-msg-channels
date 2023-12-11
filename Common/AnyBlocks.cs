using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace Channels
{
    public static class AnyBlocks
    {
        public static IPropagatorBlock<Packet, Packet> Echo<Packet>(Func<Packet, Packet>? cloning = null, DataflowBlockOptions? opt = null)
            => new BroadcastBlock<Packet>(cloning ?? (p => p), opt ?? EchoOptions);

        public static IPropagatorBlock<Packet, Packet> Filter<Packet>(Predicate<Packet> predicate, Func<Packet, Packet>? cloning = null)
        {
            var src = new BroadcastBlock<Packet>(cloning ?? (x => x));
            var tgt = new ActionBlock<Packet>(x => { if (predicate(x)) src.Post(x); });
            return DataflowBlock.Encapsulate(tgt, src);
        }

        public static ReliefBlock<Packet> DropRejected<Packet>(ITargetBlock<Packet>? dropTarget = null)
            => new ReliefBlock<Packet>(dropTarget);
        public static ReliefBlock<Packet> Switch<Packet>(ITargetBlock<Packet>? dropTarget = null)
            => new ReliefBlock<Packet>(dropTarget);

        internal static DataflowBlockOptions EchoOptions = new() { };
        internal static ExecutionDataflowBlockOptions KeepActionOptions = new()
        {
            EnsureOrdered = true,
            MaxDegreeOfParallelism = 1,
            MaxMessagesPerTask = 1,
        };
    }

    public sealed class KeepLatestN<Packet> : BufferBased<Packet>
    {
        public int KeepLatestBound = -1;
        public KeepLatestN(int numLatest = -1)
        {
            KeepLatestBound = numLatest;
        }

        public override DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, Packet messageValue, ISourceBlock<Packet>? source, bool consumeToAccept)
        {
            if (KeepLatestBound > 0)
                while (_bufferBlock.Count + 1 > KeepLatestBound)
                    if(!_bufferBlock.TryReceive(out var _))
                        break;
            return ((ITargetBlock<Packet>)_bufferBlock).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }
    }

    public sealed class ReliefBlock<Packet> : BufferBased<Packet>
    {
        public ITargetBlock<Packet> DropTarget
        {
            get => _dropTarget;
            set
            {
                _dropTarget = value ?? DataflowBlock.NullTarget<Packet>();
                RelinkDropTarget();
            }
        }

        private ITargetBlock<Packet> _dropTarget;
        private IDisposable? _dropLink;

        internal static DataflowLinkOptions DropLinkOptions = new() { Append = true };

        public ReliefBlock(DataflowBlockOptions? options = null) : this(null, options) { }
        public ReliefBlock(ITargetBlock<Packet>? dropTarget, DataflowBlockOptions? options = null)
            : base(options)
        {
            _dropTarget = dropTarget ?? DataflowBlock.NullTarget<Packet>();
            RelinkDropTarget();
        }

        private void RelinkDropTarget()
        {
            _dropLink?.Dispose();
            _dropLink = _bufferBlock.LinkTo(_dropTarget, DropLinkOptions);
        }

        public override IDisposable LinkTo(ITargetBlock<Packet> target, DataflowLinkOptions linkOptions)
        {
            var link = _bufferBlock.LinkTo(target, linkOptions);
            RelinkDropTarget();
            return link;
        }
    }

    public abstract class BufferBased<Packet> : IPropagatorBlock<Packet, Packet>, IReceivableSourceBlock<Packet>
    {
        protected BufferBlock<Packet> _bufferBlock;

        internal static DataflowBlockOptions DefaultOptions = new() { };

        public BufferBased(DataflowBlockOptions? options = null)
        {
            _bufferBlock = new(options ?? DefaultOptions);
        }

        public Task Completion => _bufferBlock.Completion;

        public virtual Packet? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<Packet> target, out bool messageConsumed)
            => ((ISourceBlock<Packet>)_bufferBlock).ConsumeMessage(messageHeader, target, out messageConsumed);

        public virtual IDisposable LinkTo(ITargetBlock<Packet> target, DataflowLinkOptions linkOptions)
            => _bufferBlock.LinkTo(target, linkOptions);

        public virtual void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<Packet> target)
            => ((ISourceBlock<Packet>)_bufferBlock).ReleaseReservation(messageHeader, target);

        public virtual bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<Packet> target)
            => ((ISourceBlock<Packet>)_bufferBlock).ReserveMessage(messageHeader, target);

        public virtual DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, Packet messageValue, ISourceBlock<Packet>? source, bool consumeToAccept)
            => ((ITargetBlock<Packet>)_bufferBlock).OfferMessage(messageHeader, messageValue, source, consumeToAccept);

        public virtual void Complete()
            => _bufferBlock.Complete();

        public virtual void Fault(Exception exception)
            => ((IDataflowBlock)_bufferBlock).Fault(exception);

        public virtual bool TryReceive(Predicate<Packet>? filter, [MaybeNullWhen(false)] out Packet item)
            => _bufferBlock.TryReceive(filter, out item);

        public virtual bool TryReceiveAll([NotNullWhen(true)] out IList<Packet>? items)
            => _bufferBlock.TryReceiveAll(out items);
    }
}
