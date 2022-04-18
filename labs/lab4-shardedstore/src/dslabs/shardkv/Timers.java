package dslabs.shardkv;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
}

// Your code here...
@Data
final class QueryTimer implements Timer {
    static final int QUERY_PERIODIC_TIMER = 25;
}
