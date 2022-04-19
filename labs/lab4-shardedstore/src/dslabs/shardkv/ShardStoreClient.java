package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.shardkv.QueryTimer.QUERY_PERIODIC_TIMER;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private SingleKeyCommand command;
    private int sequenceNum = -1;
    private Result result;
    private final int querySeqNum = -4;
    private final Query query = new Query(-1);
    // ShardConfig is Map<Integer, Pair<Set<Address>, Set<Integer>>>
    private ShardConfig shardConfig = null;
    private Set<Address> serversGroup = null;


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

        if (command instanceof SingleKeyCommand) {
            this.command = (SingleKeyCommand) command;
            this.result = null;

            // get shardNum and then find the serversGroup responsible for that shard
            int shardNo = keyToShard(this.command.key());
            int groupId = findServersGroup(shardNo);
            this.serversGroup = shardConfig.groupInfo().get(groupId).getLeft();

            // broadcast client request to serversGroup
            // broadcast command to all server
            sequenceNum++;
            for (Address server : serversGroup) {
                send(new ShardStoreRequest(new AMOCommand(command, sequenceNum, this.address())), server);
            }

            // set a timer
            set(new ClientTimer(command), CLIENT_RETRY_MILLIS);

        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        // Your code here...
        if (m.result().sequenceNum == sequenceNum) {
            if (m.result().result instanceof Error) {
                broadcastToShardMasters(new PaxosRequest(new AMOCommand(query, querySeqNum, this.address())));
            } else {
                result = m.result().result;
                notify();
            }
        }

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
        if (command.equals(t.command()) && result == null) {
            for (Address server : serversGroup) {
                send(new PaxosRequest(new AMOCommand(command, sequenceNum, this.address())), server);
            }
            set(t, CLIENT_RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        utilities
       -----------------------------------------------------------------------*/
    private int findServersGroup(int shardNo) {
        for (int groupId : shardConfig.groupInfo().keySet()) {
            Pair<Set<Address>, Set<Integer>> serversToShards = shardConfig.groupInfo().get(groupId);
            if (serversToShards.getRight().contains(shardNo)) {
                return groupId;
            }
        }
        return -1;
    }


}
