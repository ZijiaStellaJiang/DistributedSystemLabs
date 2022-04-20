package dslabs.paxos;

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
    private final int serverId;
}

@Data
final class ElectionResponse implements Message {
    private final int serverId;
}

@Data
final class LeaderAnnounce implements Message {
    private final int roundNum;
    private final int serverId;
}