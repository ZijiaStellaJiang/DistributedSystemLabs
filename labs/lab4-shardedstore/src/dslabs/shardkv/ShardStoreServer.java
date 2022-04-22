package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.underlyingPaxos.Lab4PaxosServer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardkv.QueryTimer.QUERY_PERIODIC_TIMER;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // Your code here...
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;
    private Map<Integer, ShardState> myShardStates = null;
    private final int querySeqNum = -4;
    private final Query queryForLatestConfig = new Query(-1);
    private final Query queryForInitialConfig = new Query(0);
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
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(queryForLatestConfig, querySeqNum, this.address())));
        QueryTimer queryT = new QueryTimer();
        set(queryT, QUERY_PERIODIC_TIMER);

    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
        if (m.command().command instanceof SingleKeyCommand) {
            SingleKeyCommand command = (SingleKeyCommand) m.command().command;

            // get shardNum and then find the serversGroup responsible for that shard
            int shardNo = keyToShard(command.key());
            if (myShardStates == null || shardConfig == null) {
                // require initial config from shardMaster
                broadcastToShardMasters(new PaxosRequest(new AMOCommand(queryForInitialConfig, querySeqNum, this.address())));
                return;
            }

            int targetGroupId = findServersGroup(shardNo);

            // if the responsible serverGroup is not my ID, send Error message to client
            if (targetGroupId != this.groupId) {
                send(new ShardStoreReply(new AMOResult(new Error(), m.command().sequenceNum)), sender);
            }
            else {
                // send message to underlying paxos - handleMessage
                handleMessage(new ShardServerInternalRequest(m.command()), paxosAddress);
            }

        }
    }


    // handle query result from paxosed shardMaster to update local shardConfig state
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        Result r = m.result().result;
        if (r instanceof ShardConfig) {
            ShardConfig newShardConfig = (ShardConfig) r;
            if (shardConfig == null || newShardConfig.configNum() > shardConfig.configNum()) {
                this.shardConfig = newShardConfig;
                if (myShardStates == null && newShardConfig.configNum() == 0) {
                    initializeMyShardStates(newShardConfig.groupInfo());
                }
            }
        }
    }

    // Your code here...
    // handle message from underlying paxos
    private void handleShardServerInternalReply(ShardServerInternalReply m) {
        int sequenceNum = m.command().sequenceNum;
        if (sequenceNum != -4) {
            if (m.command().command instanceof SingleKeyCommand) {
                SingleKeyCommand command = (SingleKeyCommand) m.command().command;

                int shardId = keyToShard(command.key());
                ShardState targetShardState = myShardStates.get(shardId);
                AMOResult r = targetShardState.app().execute(m.command());
                send(new ShardStoreReply(r), m.command().sender);
            }
        } else {
            // shards exchange between server groups
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onQueryTimer(QueryTimer t) {
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(queryForLatestConfig, querySeqNum, this.address())));
        set(t, QUERY_PERIODIC_TIMER);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private int findServersGroup(int shardNo) {
        for (int groupId : shardConfig.groupInfo().keySet()) {
            Pair<Set<Address>, Set<Integer>>
                    serversToShards = shardConfig.groupInfo().get(groupId);
            if (serversToShards.getRight().contains(shardNo)) {
                return groupId;
            }
        }
        return -1;
    }

    /**
     *
     * @param groupInfo:groupId -> <group members, shard numbers>
     */
    private void initializeMyShardStates(Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo) {
        myShardStates = new HashMap<>();
        Set<Integer> myShards = groupInfo.get(groupId).getRight();
        for (Integer shardId : myShards) {
            myShardStates.put(shardId, new ShardState(shardId));
        }
    }

}
