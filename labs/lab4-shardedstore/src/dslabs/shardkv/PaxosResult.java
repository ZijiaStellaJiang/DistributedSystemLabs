package dslabs.shardkv;

import dslabs.framework.Message;
import dslabs.paxos.AMOCommand;
import lombok.Data;

@Data
public final class PaxosResult implements Message {
    private final AMOCommand amoCommand;
}
