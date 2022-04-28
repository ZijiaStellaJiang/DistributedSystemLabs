package dslabs.shardkv;

import dslabs.framework.Command;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import lombok.Data;

@Data
final class ReconfigCommand implements Command {
    private final ShardConfig shardConfig;
}
