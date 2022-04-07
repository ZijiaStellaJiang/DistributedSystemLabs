package dslabs.paxos;

import dslabs.framework.Command;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Log {
    public ProposalNum proposalNum;
    public int slotNum;
    public PaxosLogSlotStatus status;
    public Command command;
}
