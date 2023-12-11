using System.Threading.Tasks.Dataflow;

namespace Channels.Protocols
{
    using MsgIO = IPropagatorBlock<Message, Message>;
    public static class MavLink
    {
        public class HeartbeatLooper
        {
            public Action<HeartbeatLooper>? Received;
            public MAVLink.mavlink_heartbeat_t? LatestReceived { get => _latestReceived; }
            public DateTime? LatestRxTime { get => _latestRxTime; }
            public TimeSpan? LatestRxInterval { get => _latestRxInterval; }
            public EndPoint? LatestSender { get => _latestSender; }

            public MAVLink.mavlink_heartbeat_t SendData;
            public EndPoint LocalEndPoint;
            public TimeSpan Interval;

            public bool Running { get => !_loop?.IsCompleted ?? false; }

            public static TimeSpan DefaultInterval = TimeSpan.FromSeconds(1);

            public HeartbeatLooper(Channel mavlinkChannel)
                : this(mavlinkChannel.IO)
            {
                mavlinkChannel.TargetConnected += async (ch, src) =>
                {
                    await Start();
                };
                mavlinkChannel.TargetDisconnected += async (ch, src) =>
                {
                    await Stop();
                };
            }

            public HeartbeatLooper(MsgIO io)
            {
                _io = io;
                LocalEndPoint = EndPoint.Server;
                SendData = Message.ServerHearbeatData;
                Interval = DefaultInterval;
            }

            ~HeartbeatLooper() { Stop().Wait(); }

            private MsgIO _io;
            private Task? _loop = null;
            private bool _active = false;
            private MAVLink.mavlink_heartbeat_t? _latestReceived;
            private DateTime? _latestRxTime;
            private TimeSpan? _latestRxInterval;
            private EndPoint? _latestSender;

            public async Task Start()
            {
                if (Running)
                    await Stop();
                lock(this)
                {
                    _active = true;
                    var receiving = _io.OnMessage("HEARTBEAT", msg =>
                    {
                        var rxTime = DateTime.Now;
                        _latestRxInterval = rxTime - _latestRxTime;
                        _latestRxTime = rxTime;

                        _latestSender = msg.Sender as EndPoint;
                        _latestReceived = msg.Data as MAVLink.mavlink_heartbeat_t?;

                        Received?.Invoke(this);
                    });
                    _loop = Task.Run(async () =>
                    {
                        while(_active)
                        {
                            await Task.Delay(Interval);

                            var heartbeat = new Message(MAVLink.MAVLINK_MSG_ID.HEARTBEAT)
                            {
                                Data = SendData,
                                Sender = LocalEndPoint,
                            };
                            try
                            {
                                if (!await _io.SendAsync(heartbeat))
                                    break;
                            } catch (Exception e) {
                                Console.WriteLine(e);
                            }
                        }
                    });
                    _loop.ContinueWith(t =>
                    {
                        _active = false;
                        _loop = null;
                        receiving.Dispose();
                    });
                }
            }

            public Task Stop()
            {
                lock (this)
                {
                    if (!Running)
                        return Task.CompletedTask;
                    _active = false;
                    if (_loop != null)
                        return _loop;
                    return Task.CompletedTask;
                }
            }
        }

        public class Message : Channels.Message
        {
            public MAVLink.MAVLINK_MSG_ID TypeId;
            [NonSerialized()] public MAVLink.MAVLinkMessage? MavSrc;
            [NonSerialized()] public MAVLink.message_info? MavInfo;

            public override bool IsValid()
                => Type != "" && ((uint)TypeId != uint.MaxValue)
                && (MavInfo != null) && (Data.GetType() == MavInfo?.type);

            public Message() { }
            public Message(MAVLink.MAVLinkMessage mavMsg)
            {
                Parse(mavMsg);
            }

            public Message(MAVLink.MAVLINK_MSG_ID typeId) : this((uint)typeId) { }
            public Message(uint typeId)
            {
                SetTypeId(typeId);
            }
            public Message(string typeName)
            {
                SetTypeName(typeName);
            }

            public override bool FromBytes(byte[] packet)
            {
                var mavMsg = new MAVLink.MAVLinkMessage(packet);
                return Parse(mavMsg);
            }
            public override byte[] ToBytes() {
                if (!IsValid()) return new byte[0];
                EndPoint ep = Sender as EndPoint ?? EndPoint.Server;
                return _parser.GenerateMAVLinkPacket20(TypeId, Data, false, ep.System, ep.Component);
            }
            public override bool ReadFrom(Stream stream)
            {
                var mavMsg = _parser.ReadPacket(stream);
                if (mavMsg == null)
                    return false;
                return Parse(mavMsg);
            }

            public bool Parse(MAVLink.MAVLinkMessage mavMsg)
            {
                if (mavMsg == MAVLink.MAVLinkMessage.Invalid)
                    return false;
                MavSrc = mavMsg;
                SetTypeId(mavMsg.msgid);
                Data = mavMsg.data;
                Sender = EndPoint.Extract(mavMsg);
                return IsValid();
            }

            private void SetTypeId(uint typeId)
            {
                var info = MAVLink.MAVLINK_MESSAGE_INFOS.GetMessageInfo(typeId);
                SetMavInfo(info);
            }

            private void SetTypeName(string msgname)
            {
                var info = MAVLink.MAVLINK_MESSAGE_INFOS.GetMessageInfo(msgname);
                SetMavInfo(info);
            }

            private void SetMavInfo(MAVLink.message_info info)
            {
                if (info.msgid != uint.MaxValue)
                {
                    MavInfo = info;
                    Type = info.name;
                    TypeId = (MAVLink.MAVLINK_MSG_ID)info.msgid;
                }
            }

            private static MAVLink.MavlinkParse _parser = new MAVLink.MavlinkParse();

            public static MAVLink.mavlink_heartbeat_t ServerHearbeatData = new()
            {
                autopilot = (byte)MAVLink.MAV_AUTOPILOT.INVALID,
                type = (byte)MAVLink.MAV_TYPE.GCS,
                base_mode = (byte)MAVLink.MAV_MODE_FLAG.CUSTOM_MODE_ENABLED,
                system_status = (byte)MAVLink.MAV_STATE.ACTIVE,
                mavlink_version = MAVLink.MAVLINK_VERSION
            };
        }

        public class EndPoint : Channels.EndPoint
        {
            public byte Component;
            public byte System;

            public override bool Match(Channels.EndPoint? other)
            {
                var oth = other as EndPoint;
                return oth != null && oth.Component == Component && oth.System == System;
            }

            public static EndPoint Extract(MAVLink.MAVLinkMessage mavMsg)
            {
                return new EndPoint { System = mavMsg.sysid, Component = mavMsg.compid };
            }

            public static readonly EndPoint Server = new EndPoint
            {
                Component = 2, // MAV_COMP_ID_BASE_SERVER
                System = 1 // MAV_SYS_ID_BASE_STATION
            };
        }


        private static TimeSpan DefaultHandshakeTimeout = TimeSpan.FromSeconds(5);
        public static Func<Connector.Connection, Task<Connector.Connection>> HandshakeDelegate(EndPoint? local = null, TimeSpan? timeout = null)
        {
            EndPoint ep = local ?? EndPoint.Server;
            var my_heartbeat = new Message(MAVLink.MAVLINK_MSG_ID.HEARTBEAT)
            {
                Data = Message.ServerHearbeatData,
                Sender = ep,
            };
            return (async(con) =>
            {
                try
                {
                    con.LocalAddr = ep.CloneOrigin(con.LocalAddr);
                    if (con.Output != null)
                        if (!await con.Output.SendAsync(my_heartbeat))
                            con.IsRejected = true;

                    if (con.Input != null)
                    {
                        var task = con.Input.ReceiveAsync(timeout ?? DefaultHandshakeTimeout);
                        await task;
                        if (task.IsCanceled || task.IsFaulted)
                            con.IsRejected = true;
                        else
                            con.RemoteAddr = task.Result.Sender?.WithOrigin(con.RemoteAddr) ?? con.RemoteAddr;
                    }
                } catch
                {
                    con.IsRejected = true;
                }

                return con;
            });
        }
    }
}

