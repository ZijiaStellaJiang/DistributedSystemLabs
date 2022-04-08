package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Log {
    public ProposalNum proposalNum;
    public int slotNum;
    public PaxosLogSlotStatus status;
    public AMOCommand command;
}
