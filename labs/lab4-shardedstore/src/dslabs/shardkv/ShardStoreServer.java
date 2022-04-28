package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Result;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.underlyingPaxos.Lab4PaxosServer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardkv.QueryTimer.QUERY_PERIODIC_TIMER;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // Your code here...
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;
    private Map<Integer, ShardState> myShardStates;
    private final int querySeqNum = -4;
    private final Query query = new Query(-1);
    // ShardConfig is Map<Integer, Pair<Set<Address>, Set<Integer>>>
    private ShardConfig shardConfig = null;

    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // Your code here...
        this.myShardStates = new HashMap<>();
    }



    @Override
    public void init() {
        // Your code here...
        // create a PaxosServer (using Lab4PaxosServer) and initialize it
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        Lab4PaxosServer paxosServer =
                new Lab4PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        // set Query send periodically to ShardMaster
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(query, querySeqNum, this.address())));
        QueryTimer queryT = new QueryTimer();
        set(queryT, QUERY_PERIODIC_TIMER);

    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
    }


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

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
