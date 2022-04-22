package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Application;
import dslabs.kvstore.KVStore;
import lombok.Data;

@Data
public class ShardState {
    private int shardId;
    private AMOApplication<Application> app;


    public ShardState(int shardId) {
        this.shardId = shardId;
        this.app = new AMOApplication<>(new KVStore());
    }

}
