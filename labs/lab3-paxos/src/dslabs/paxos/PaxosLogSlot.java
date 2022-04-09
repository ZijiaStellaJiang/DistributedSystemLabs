package dslabs.paxos;

import dslabs.framework.Address;
import java.util.Set;
import lombok.Data;

@Data
public class PaxosLogSlot{
    private PaxosLogSlotStatus status;
    private Set<Address> acceptors;
    private AMOCommand amoCommand;
}
