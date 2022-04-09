package dslabs.paxos;
import java.io.Serializable;
import lombok.Data;

@Data
public class ProposalNum implements Serializable {
    public final int roundNum;
    public final int ServerID;

    public ProposalNum(int roundNum, int serverID) {
        this.roundNum = roundNum;
        ServerID = serverID;
    }
}
