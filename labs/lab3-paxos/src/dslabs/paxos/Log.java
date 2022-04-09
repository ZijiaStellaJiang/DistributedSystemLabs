package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Log implements Serializable {
    public ProposalNum proposalNum;
    public int slotNum;
    public PaxosLogSlotStatus status;
    public AMOCommand command;
}
