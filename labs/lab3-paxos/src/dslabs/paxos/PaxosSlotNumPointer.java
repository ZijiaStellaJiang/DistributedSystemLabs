package dslabs.paxos;

import lombok.Data;

@Data
public class PaxosSlotNumPointer {
    private int firstNonClearedSlotNum = 1;
    private int firstUnchosenSlotNum = 1;
    private int firstEmptySlotNum = 1;
    private int lastNonEmptySlotNum = 0;
    private int executeToSlotNum = 0;
}
