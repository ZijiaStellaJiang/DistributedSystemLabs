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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private ShardExchanger shardExchanger = null;
    private boolean inReconfig = false;

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
        if (inReconfig) return;
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
    private void handlePaxosReply(PaxosReply m, Address sender) {
        Result r = m.result().result;
        if (r instanceof ShardConfig) {
            ShardConfig newShardConfig = (ShardConfig) r;
            if (shardConfig == null || newShardConfig.configNum() > shardConfig.configNum()) {
                if (myShardStates == null && newShardConfig.configNum() == 0) {
                    this.shardConfig = newShardConfig;
                    initializeMyShardStates(newShardConfig.groupInfo());
                }else if(!inReconfig && shardExchanger == null){
                    // send reconfig command to paxos for consensus
                    handleMessage(new ShardServerInternalRequest(new AMOCommand(new ReconfigCommand(newShardConfig),querySeqNum,this.address())),paxosAddress);
                    inReconfig = true;
                    //setShardExchanger(buildShardExchanger(newShardConfig,myShardStates));
                }
            }
        }
    }

    // Your code here...
    // handle message from underlying paxos
    private void handleShardServerInternalReply(ShardServerInternalReply m, Address sender) {
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
            // paxos 达成共识，store server执行
            // 向需要shard的server发送shardState
            if (m.command().command instanceof ReconfigCommand) {
                ReconfigCommand reconfigCommand = (ReconfigCommand) m.command().command;
                if (inReconfig && shardExchanger == null && reconfigCommand.shardConfig().configNum() == shardConfig.configNum()+1) {
                    // build shardExchanger according to new config
                    setShardExchanger(reconfigCommand);
                }
            }
        }
    }

    private void handleShardPack(ShardPack sp, Address sender) {
        if (sp.configNum() == shardConfig.configNum() && shardExchanger != null) {
            for (ShardState shardState : sp.shards()) {
                int shardId = shardState.shardId();
                Set<Address> trustedSender = shardExchanger.receiveFrom().getOrDefault(shardId, new HashSet<>());
                if (trustedSender.contains(sender)) {
                    myShardStates.put(shardId, shardState);
                    shardExchanger.receiveFrom().remove(shardId);
                    for (Address provider : trustedSender) {
                        send(new ShardReceipt(shardConfig.configNum()), provider);
                    }
                }
            }
        } else if (sp.configNum() == shardConfig.configNum() && shardExchanger == null) {
            send(new ShardReceipt(shardConfig.configNum()), sender);
        }
    }

    private void handleShardReceipt(ShardReceipt sr, Address sender) {
        if (shardConfig.configNum() == sr.configNum() && shardExchanger!=null) {
            for (Set<Address> addresses : new HashSet<>(shardExchanger.sendTo().keySet())) {
                // find the group and remove sender from the toSend set
                if (addresses.contains(sender)) {
                    Pair<Set<Address>,Set<ShardState>> pair = shardExchanger.sendTo().get(addresses);
                    pair.getLeft().remove(sender);
                    shardExchanger.sendTo().put(addresses,pair);
                }
            }
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

    private void onExchangeTimer(ExchangeTimer t) {
        ShardExchanger se = this.shardExchanger;
        if (shardExchanger.sendTo().keySet().size() == 0 && shardExchanger.receiveFrom().size() == 0) {
            shardExchanger = null;
            inReconfig = false;
        } else {
            if (shardExchanger.sendTo().keySet().size() != 0) {
                sendShards();
            }
            set(t,ExchangeTimer.EXCHANGE_MILLIS);
        }
    }
    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private void setShardExchanger(ReconfigCommand reconfigCommand){
        // find difference between shardConfig and myShardStates
        // to build shardExchanger
        shardExchanger = buildShardExchanger(reconfigCommand.shardConfig(), myShardStates);
        this.shardConfig = reconfigCommand.shardConfig();
        sendShards();
        set(new ExchangeTimer(), ExchangeTimer.EXCHANGE_MILLIS);
    }

    private void sendShards() {
        Map<Set<Address>, Pair<Set<Address>,Set<ShardState>>> sendTo = new HashMap<>(shardExchanger.sendTo());
        for (Set<Address>  addresses: sendTo.keySet()) {
            ShardPack sp = new ShardPack(shardExchanger.configNum(), sendTo.get(addresses).getRight());
            Set<Address> toSendAddresses = sendTo.get(addresses).getLeft();
            if (!toSendAddresses.isEmpty()) {
                for (Address address : toSendAddresses) {
                    send(sp,address);
                }
            } else {
                // once everyone received, remove from local and don't send anymore
                for (ShardState shardState : sendTo.get(addresses).getRight()) {
                    myShardStates.remove(shardState.shardId());
                }
                shardExchanger.sendTo().remove(addresses);
            }
        }
    }

    private ShardExchanger buildShardExchanger(ShardConfig newShardConfig, Map<Integer, ShardState> myShardStates) {
        ShardExchanger se = new ShardExchanger(newShardConfig.configNum());
        Set<Integer> newShards = newShardConfig.groupInfo().getOrDefault(groupId, Pair.of(new HashSet<>(), new HashSet<>())).getRight();
        Map<ShardState, Set<Address>> toSend = new HashMap<>();
        for (int shardId : myShardStates.keySet()) {
            if (!newShards.contains(shardId)) {
                // record in shardExchanger as toSend
                toSend.put(myShardStates.get(shardId), getServerAddress(newShardConfig,shardId));
            }
        }
        Map<Set<Address>, Pair<Set<Address>,Set<ShardState>>> sendTo = new HashMap<>();
        for (ShardState shardState : toSend.keySet()) {
            Pair<Set<Address>,Set<ShardState>> pair= sendTo.getOrDefault(toSend.get(shardState), Pair.of(new HashSet<>(toSend.get(shardState)), new HashSet<>()));
            pair.getRight().add(shardState);
            sendTo.put(toSend.get(shardState), pair);
        }
        se.sendTo(sendTo);
        Map<Integer, Set<Address>> receiveFrom = new HashMap<>();
        for (int shardId : newShards) {
            if (!myShardStates.containsKey(shardId)) {
                // record receiveFrom according to current shardConfig
                receiveFrom.put(shardId, getServerAddress(this.shardConfig, shardId));
            }
        }
        se.receiveFrom(receiveFrom);
        return se;
    }

    private Set<Address> getServerAddress(ShardConfig shardConfig, int shardId) {
        for (int groupId : shardConfig.groupInfo().keySet()) {
            Pair<Set<Address>, Set<Integer>>
                    serversToShards = shardConfig.groupInfo().get(groupId);
            if (serversToShards.getRight().contains(shardId)) {
                return new HashSet<>(serversToShards.getLeft());
            }
        }
        return new HashSet<>();
    }

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
        Pair<Set<Address>, Set<Integer>> pair = groupInfo.get(groupId);
        if (pair == null) {
            return;
        }
        Set<Integer> myShards = pair.getRight();
        for (Integer shardId : myShards) {
            myShardStates.put(shardId, new ShardState(shardId));
        }
    }

}
