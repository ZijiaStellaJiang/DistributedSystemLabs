package dslabs.paxos;

import lombok.Data;
import lombok.NonNull;

@Data
public class PaxosLogSlot implements Comparable<PaxosLogSlot>{
    private final int round;
    private final int index;
    private PaxosLogSlotStatus status;
    private AMOCommand amoCommand;

    @Override
    public int compareTo(@NonNull PaxosLogSlot rhs) {
        if (this.round == rhs.round){
            return this.index - rhs.index;
        }else {
            return this.round - rhs.round;
        }
    }
}
