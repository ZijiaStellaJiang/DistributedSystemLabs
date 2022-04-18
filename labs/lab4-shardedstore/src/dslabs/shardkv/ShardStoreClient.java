package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.checkerframework.checker.units.qual.A;

import static dslabs.shardkv.QueryTimer.QUERY_PERIODIC_TIMER;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private Command command;
    private Result result;
    private final int querySeqNum = -4;
    private final Query query = new Query(-1);
    // ShardConfig is Map<Integer, Pair<Set<Address>, Set<Integer>>>
    private ShardConfig shardConfig = null;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // Your code here...
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(query, querySeqNum, this.address())));
        QueryTimer queryT = new QueryTimer();
        set(queryT, QUERY_PERIODIC_TIMER);
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return false;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        return null;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        // Your code here...

    }

    // Your code here...
    // handle query result from paxosed shardMaster to update local shardConfig state
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        Result r = m.result().result;
        if (r instanceof ShardConfig) {
            ShardConfig newShardConfig = (ShardConfig) r;
            if (shardConfig == null || newShardConfig.configNum() > shardConfig.configNum()) {
                this.shardConfig = newShardConfig;
            }
        }
    }
    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
    }

}
