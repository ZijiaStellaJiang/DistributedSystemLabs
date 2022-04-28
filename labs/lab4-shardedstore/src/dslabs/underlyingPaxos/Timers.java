package dslabs.underlyingPaxos;


import dslabs.framework.Command;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 15;

    // Your code here...
    private final Command command;
}

// Your code here...
@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 25;

    private boolean leaderTolerate = true;
}

@Data
final class ProposerTimer implements Timer {
    static final int PROPOSER_RETRY_MILLIS = 10;
    private final ProposerRequest pReq;
}
