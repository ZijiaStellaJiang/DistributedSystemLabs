package dslabs.paxos;

// Your code here...
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Message;
import java.util.Map;
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
    - usage1: leader election (roundNum, voteID)
    - usage2: communicate between leader and followers (to validate leader aliveness, firstUnchosenIndex)
   -----------------------------------------------------------------------*/
@Data
class Heartbeat implements Message {
    private final int roundNum;
    private final int senderID;
    private final Address senderAddress;
    private final int voteID;
    private final int firstUnchosenIndex;
    private final Map<Address, Integer> server_FirstUnchosenIndex;
    private final Map<Address, Log> server_LogForFirstUnchosenIndex;
}

/* -------------------------------------------------------------------------
    Proposal Messages
   -----------------------------------------------------------------------*/
@Data
class ProposerRequest implements Message {
    private final ProposalNum proposalNum;
    private final int slotNum;
    private final AMOCommand localAcceptedCommand;
    private final AMOCommand clientCommand;
}

@Data
class AcceptorReply implements Message {
    private final ProposalNum proposalNum;
    private final int slotNum;
    private final boolean acceptProposal;
    private final AMOCommand alreadyAcceptedCommand;

}

/* -------------------------------------------------------------------------

   -----------------------------------------------------------------------*/
