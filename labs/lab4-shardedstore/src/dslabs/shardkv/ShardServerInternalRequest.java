package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class ShardServerInternalRequest implements Message{
    private final AMOCommand command;
}

