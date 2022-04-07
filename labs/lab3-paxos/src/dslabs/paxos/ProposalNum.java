package dslabs.paxos;


public class ProposalNum {
    public final int roundNum;
    public final int ServerID;

    public ProposalNum(int roundNum, int serverID) {
        this.roundNum = roundNum;
        ServerID = serverID;
    }
}
