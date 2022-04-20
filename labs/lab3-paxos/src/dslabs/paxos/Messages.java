package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

@Data
final class LeaderMessage implements Message{
    private final Map<Integer, PaxosLogSlot> log;
    private final PaxosSlotNumPointer slotNumPointer;
}

@Data
final class FollowerMessage implements Message{
    private final Map<Integer, PaxosLogSlot> log;
    private final PaxosSlotNumPointer slotNumPointer;
}

@Data
final class ElectionRequest implements Message {
    private final int roundNum;
    private final int potentialLeader;
}

@Data
final class LeaderAck implements Message {
    private final int roundNum;
    private final Address follower;
}

@Data
final class LeaderAnnounce implements Message {
    private final int roundNum;
    private final int serverId;
    private final Address leaderAddress;
}