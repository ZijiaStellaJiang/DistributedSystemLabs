package dslabs.paxos;

import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final Command command;
}

// Your code here...
@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 25;
}