package dslabs.paxos;

import dslabs.framework.Address;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PaxosLogSlot implements Comparable<PaxosLogSlot>{
    private int roundNum;
    private int serverId;
    private int slotNum;
    private PaxosLogSlotStatus status;
    private Set<Address> acceptors;
    private AMOCommand amoCommand;


    @Override
    public int compareTo(PaxosLogSlot rhs) {
        if (roundNum == rhs.roundNum){
            return serverId - rhs.serverId;
        } else {
            return roundNum - rhs.roundNum;
        }
    }
}
