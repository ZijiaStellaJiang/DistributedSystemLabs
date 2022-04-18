package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class ShardState {
    private int shardNo;
    private KVStore kvStore;
    private Map<Address, Result> lastResultMap;

    public ShardState(int shardNo) {
        this.shardNo = shardNo;
        this.kvStore = new KVStore();
        this.lastResultMap = new HashMap<>();
    }

    public ShardState(int shardNo, KVStore kvStore, Map<Address, Result> lastResultMap) {
        this.shardNo = shardNo;
        this.kvStore = kvStore;
        this.lastResultMap = lastResultMap;
    }
}
