using System.Threading.Tasks.Dataflow;

namespace Channels
{
    using MsgTransform = TransformBlock<Message, Message>;

    using MsgToMsg = Func<Message, Message>;
    using OnMsg = Action<Message>;
    using MsgToMsgAsync = Func<Message, Task<Message>>;
    using OnMsgAsync = Func<Message, Task>;
    using System.Threading.Tasks.Dataflow;

    public static class MsgBlocks
    {
        public static BufferBlock<Message> Buffer() => new BufferBlock<Message>();
        public static BroadcastBlock<Message> Broadcast(MsgToMsg? clonning = null) => new BroadcastBlock<Message>(clonning ?? (m => m));
        public static ActionBlock<Message> Action(OnMsg callback) => new ActionBlock<Message>(callback);
        public static ActionBlock<Message> Action(OnMsgAsync callback) => new ActionBlock<Message>(callback);
        public static MsgTransform Transform(MsgToMsg func) => new MsgTransform(func);
        public static MsgTransform Transform(MsgToMsgAsync func) => new MsgTransform(func);
        public static TransformBlock<Message, byte[]> ToBuffers() => new TransformBlock<Message, byte[]>(Message.MessageToBytes);
        public static TransformBlock<byte[], Message> FromBuffers<MsgClass>() where MsgClass : Message, new()
            => new TransformBlock<byte[], Message>(Message.BytesToMessage<MsgClass>);
        public static TransformBlock<Message, T> FromMessages<T>(Func<Message, T> func) => new TransformBlock<Message, T>(func);
        public static TransformBlock<T, Message> ToMessages<T>(Func<T, Message> func) => new TransformBlock<T, Message>(func);
        public static TransformBlock<Message, T> FromMessages<T>(Func<Message, Task<T>> func) => new TransformBlock<Message, T>(func);
        public static TransformBlock<T, Message> ToMessages<T>(Func<T, Task<Message>> func) => new TransformBlock<T, Message>(func);
    }
}
