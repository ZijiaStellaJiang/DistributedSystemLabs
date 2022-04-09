package dslabs.paxos;

import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

@Data
class LeaderMessage implements Message{
    private final Map<Integer, PaxosLogSlot> log;
    private final PaxosSlotNumPointer slotNumPointer;
}

@Data
class FollowerMessage implements Message{
    private final Map<Integer, PaxosLogSlot> log;
    private final PaxosSlotNumPointer slotNumPointer;
}