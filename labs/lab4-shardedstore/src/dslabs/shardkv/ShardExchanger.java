package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
public class ShardExchanger {
    private final int configNum;
    // all addresses -> <addresses without receipt, shardStates to send>
    private Map<Set<Address>, Pair<Set<Address>,Set<ShardState>>> sendTo;
    // shardId -> server addresses
    private Map<Integer, Set<Address>> receiveFrom;
}