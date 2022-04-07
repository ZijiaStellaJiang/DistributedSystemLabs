package dslabs.paxos;

// Your code here...
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Message;
import lombok.Data;

/* -------------------------------------------------------------------------
    Client-Server Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    private final AMOCommand command;
}

@Data
class Reply implements Message {
    private final AMOResult result;
}

/* -------------------------------------------------------------------------
    Heartbeat Messages
    - usage1: leader election (roundNum, senderID)
    - usage2: communicate between leader and followers (to validate leader aliveness, firstUnchosenIndex)
   -----------------------------------------------------------------------*/
@Data
class Heartbeat implements Message {
    private final int roundNum;
    private final int senderID;
    private final int firstUnchosenIndex;
}

/* -------------------------------------------------------------------------
    Servers Prepare Messages
   -----------------------------------------------------------------------*/



/* -------------------------------------------------------------------------
    Servers P2A Messages
   -----------------------------------------------------------------------*/
