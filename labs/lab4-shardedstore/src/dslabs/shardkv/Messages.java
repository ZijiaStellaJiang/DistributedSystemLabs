package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import java.util.Set;
import lombok.Data;

/* -------------------------------------------------------------------------
    message between ShardStoreClient and ShardStoreServer
   -----------------------------------------------------------------------*/
@Data
final class ShardStoreRequest implements Message {
    // Your code here...
    private final AMOCommand command;
}

@Data
final class ShardStoreReply implements Message {
    // Your code here...
    private final AMOResult result;
}

// Your code here...
@Data
final class ShardPack implements Message {
    private final int configNum;
    private final Set<ShardState> shards;
}

@Data
final class ShardReceipt implements Message {
    private final int configNum;
}
