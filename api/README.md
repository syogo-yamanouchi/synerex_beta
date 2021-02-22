# synerex_api
Protocol Description for Synerex

Since synerex v0.4.0 we update mbus specification.

Synerex Protocol

 4 types of messages.
    Demand
    Supply
    Target
    Mbus
       MbusMsg
       MbusOpt
       MbusState

---
Synerex API:
    rpc NotifyDemand(Demand) returns (Response) {}
    rpc NotifySupply(Supply) returns (Response) {}
    rpc ProposeDemand(Demand) returns (Response) {}
    rpc ProposeSupply(Supply) returns (Response) {}
    rpc SelectSupply(Target) returns (ConfirmResponse) {}
    rpc SelectDemand(Target) returns (ConfirmResponse) {}
    rpc Confirm(Target) returns (Response){}
    rpc SubscribeDemand(Channel) returns (stream Demand) {}
    rpc SubscribeSupply(Channel) returns (stream Supply) {}

Mbus is a selectable message bus for specific members.
    rpc SubscribeMbus(Mbus) returns (stream MbusMsg) {}
    rpc SendMsg(MbusMsg) returns (Response){}
    rpc CloseMbus(Mbus) returns (Response){}

To create new Mbus, we can ask to create new MbusID for Synerex Server
(since v0.4.0)
    rpc CreateMbus(MbsOpt) returns (Mbus){}
    rpc GetMbusState(Mbus) returns (MBusState){}

