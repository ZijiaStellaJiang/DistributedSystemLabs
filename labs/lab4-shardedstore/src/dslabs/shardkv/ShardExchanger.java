package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Command;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
public class ShardExchanger implements Command {
    private final int configNum;
    // all addresses -> <addresses without receipt, shardStates to send>
    private Map<Set<Address>, Pair<Set<Address>,Set<ShardState>>> sendTo;
    // shardId -> server addresses
    private Map<Integer, Set<Address>> receiveFrom;
}
