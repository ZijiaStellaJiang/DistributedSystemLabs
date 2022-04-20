package dslabs.paxos;

import dslabs.framework.Command;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final Command command;
}

@Data
final class HeartbeatTimer implements Timer{
    static final int HEARTBEAT_MILLIS = 50;

}

@Data
final class ElectionTimer implements Timer {
    static final int ELECTION_MILLIS = 100;

}

@Data
final class LeaderAnnounceTimer implements Timer {
    static final int LEADER_ACK_MILLIS = 100;
}