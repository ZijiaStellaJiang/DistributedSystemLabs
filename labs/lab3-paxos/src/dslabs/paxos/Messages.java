package dslabs.paxos;

import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

@Data
class LeaderMessage implements Message{
    private final Map<Integer, PaxosLogSlot> log;
}

@Data
class FollowerMessage implements Message{
    private final int firstUnchosenSlot;
}