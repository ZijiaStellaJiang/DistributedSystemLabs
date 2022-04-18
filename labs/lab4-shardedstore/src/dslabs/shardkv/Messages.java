package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
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

//
//// Your code here...
///* -------------------------------------------------------------------------
//    message between ShardStoreClient and ShardMaster
//    which is same as
//    message between ShardStoreServer and ShardMaster
//   -----------------------------------------------------------------------*/
//@Data
//final class ShardQueryRequest implements Message {
//
//}
//
//@Data
//final class ShardQueryReply implements Message {
//
//}