package dslabs.underlyingPaxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import dslabs.paxos.Log;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.ProposalNum;
import dslabs.shardkv.ShardServerInternalReply;
import dslabs.shardkv.ShardServerInternalRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import dslabs.paxos.PaxosLogSlotStatus;

import static dslabs.paxos.PaxosLogSlotStatus.CHOSEN;
import static dslabs.paxos.PaxosLogSlotStatus.CLEARED;
import static dslabs.underlyingPaxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.PaxosLogSlotStatus.ACCEPTED;
import static dslabs.paxos.PaxosLogSlotStatus.EMPTY;
import static dslabs.underlyingPaxos.ProposerTimer.PROPOSER_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Lab4PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;
    private final Address shardStoreServer;
    private final int quorum;
    private int firstUnchosenIndex;
    private Map<Integer, Log> log;
    private Map<Address, Integer> server_FirstUnchosenIndex;
    private Map<Address, Log> server_LogForFirstUnchosenIndex;
    /* note: a Proposal Number = RoundNum + ServerID */
    /* current Round Number of this server */
    private int roundNum;
    /* serverID of this server, which is this server's index in servers[] */
    private int serverID;
    private Address leaderAddress;
    private int leaderID;
    private boolean isLeaderAlive;
    private Set<Address> myVoters;
    private int voteID;
    /* gcPointer points to the first unGced log slotNum*/
    private int gcPointer;
    private ProposerRequest proposerRequest;
    private AMOCommand alreadyAcceptedFromFollowers;
    private Set<Address> proposalRepliers;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public Lab4PaxosServer(Address address, Address[] servers, Address shardStoreServer) {
        super(address);
        this.servers = servers;
        this.shardStoreServer = shardStoreServer;

        this.quorum = servers.length / 2 + 1;
        this.firstUnchosenIndex = 1;
        this.log = new HashMap<>();
        this.server_FirstUnchosenIndex = new HashMap<>();
        for (Address server: servers) {
            this.server_FirstUnchosenIndex.put(server, 0);
        }

        this.server_LogForFirstUnchosenIndex = new HashMap<>();
        this.roundNum = 0;
        this.serverID = generateServerID(address, servers);
        this.leaderAddress = servers[0];
        this.leaderID = 0;
        this.isLeaderAlive = true;
        this.myVoters = new HashSet<>();
        this.voteID = this.serverID;
        this.gcPointer = 1;
        this.proposerRequest = null;
        this.alreadyAcceptedFromFollowers = null;
        this.proposalRepliers = new HashSet<>();

    }


    @Override
    public void init() {
        // Your code here...
        if (servers.length == 1) return;

        broadcast(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex));
        HeartbeatTimer t = new HeartbeatTimer();
        t.leaderTolerate(true);
        set(t, HEARTBEAT_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardServerInternalRequest(ShardServerInternalRequest m, Address sender) {
        // only leader will process client's requests
        if (!this.address().equals(this.leaderAddress)) {
            // if no result && this server is a follower
            // redirect the message to leader
            send(m, leaderAddress);
            return;
        }

        if (isInLogToExecute(m.command())) {
            return;
        }

        if (servers.length == 1) {
            // directly send back reply to shardStoreServer to tell the command is ready to execute (even don't need log)
            send(new ShardServerInternalReply(m.command()), shardStoreServer);
            return;
        } else if (this.proposerRequest == null){
            // launch proposal --> (get majority consensus --> put the command to log & update firstUnchosenIndex)
            int slotNum = this.firstUnchosenIndex;
            AMOCommand localAcceptedCommand = getLogAMOCommand(slotNum);
            this.proposerRequest = new ProposerRequest(localAcceptedCommand == null? 1:2, new ProposalNum(roundNum, this.serverID), slotNum, localAcceptedCommand, m.command());
            broadcast(this.proposerRequest);
            set(new ProposerTimer(this.proposerRequest), PROPOSER_RETRY_MILLIS);

        }
    }

    private void handleHeartbeat(Heartbeat hb, Address sender) {

        int senderRoundNum = hb.roundNum();
        int senderID = hb.senderID();
        int senderFirstUnchosenIndex = hb.firstUnchosenIndex();

        // messages with a smaller RoundNum: ignore
        if (senderRoundNum < this.roundNum) return;

        // message with a higher RoundNum: get synchronize with new leader
        if (senderRoundNum > this.roundNum) {
            // update roundNum, leaderAddress, leaderID in its state
            this.roundNum = senderRoundNum;
            this.leaderAddress = sender;
            this.leaderID = senderID;
            this.myVoters.clear();
            this.voteID = this.serverID;
            this.proposerRequest = null;
            this.proposalRepliers.clear();

            // info will automatically synchronized with new leader as it is going to communicate with new leader via heartbeat
            // with the update state, this sever will send its heartbeat to new leader
        }
        // message with the same RoundNum as the receiver
        // If the receiver is in leader-election mode
        if (this.leaderAddress == null) {
            leaderElection(hb, sender);
            return;
        }

        // Else, the receiver is working in normal leader-follower mode
        else {
            // case1: this server (i.e., receiver) is the leader
            if (this.address().equals(this.leaderAddress)) {
                // leader also need to update its status (it is itself leader)
                this.isLeaderAlive = true;
                // help follower update firstUnchosenIndex
                leaderSyncWithFollower(hb);
            }

            // case2: this server (i.e., receiver) is a follower, it will only process message from leader in this round's normal cases
            else {
                // update leader alive status
                this.isLeaderAlive = true;
                followerSyncWithLeader(hb);
            }
        }

    }

    private void handleProposerRequest(ProposerRequest pReq, Address sender) {
        // only process proposal from leader in this round && should be in the same round
        if (sender.equals(this.leaderAddress)
                && pReq.proposalNum().roundNum == this.roundNum
                && pReq.proposalNum().ServerID == this.leaderID) {

            // if pReq.localAcceptedCommand is not null, then acceptor can accept this localAcceptedCommand
            if (pReq.localAcceptedCommand() != null) {
                setLogInPosition(pReq, ACCEPTED);
                AcceptorReply aReply = new AcceptorReply(2, new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), true, pReq.localAcceptedCommand());
                send(aReply, sender);
            }

            // else check if it has already accepted command in this slotNum
            else {
                PaxosLogSlotStatus status = status(pReq.slotNum());
                if (ACCEPTED.equals(status)) {
                    AMOCommand acceptedCommand = getLogAMOCommand(pReq.slotNum());
                    AcceptorReply aReply = new AcceptorReply(1, new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), false, acceptedCommand);
                    send(aReply, sender);
                }
                else if (EMPTY.equals(status)) {
                    AcceptorReply aReply = new AcceptorReply(1, new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), true, null);
                    send(aReply, sender);
                }
            }
        }
    }

    private void handleAcceptorReply(AcceptorReply aReply, Address sender) {
        // make sure I am the leader in this round and I only process aReply with the same slotNum
        if (this.address().equals(leaderAddress)
                && aReply.proposalNum().roundNum == this.roundNum
                && aReply.proposalNum().ServerID == this.serverID
                && aReply.slotNum() == this.firstUnchosenIndex
                && this.proposerRequest != null
                && this.proposerRequest.phaseNum() == aReply.phaseNum()
        ) {

            // once find an accepted command, I need to change my command to the already accepted command
            if (!aReply.acceptProposal()) {
                alreadyAcceptedFromFollowers = aReply.alreadyAcceptedCommand();
            }
            // update received aReply
            proposalRepliers.add(sender);

            // check if reach quorum
            if (proposalRepliers.size() + 1 >= quorum) {
                if (alreadyAcceptedFromFollowers != null) {
                    // I need to accept the alreadyAcceeptedCommand locally updating this.proposerRequest, and then use the new this.proposerRequest to broadcast
                    this.proposerRequest = new ProposerRequest(2, new ProposalNum(roundNum, this.serverID), this.proposerRequest.slotNum(), alreadyAcceptedFromFollowers, this.proposerRequest.clientCommand());
                    this.proposalRepliers.clear();
                    broadcast(this.proposerRequest);
                }
                else {
                    if (this.proposerRequest.localAcceptedCommand() != null) {
                        // done. I can update the slot status to chosen, and execute it
                        setLogInPosition(this.proposerRequest, CHOSEN);
                        updateFirstUnchosenIndex();
                        send(new ShardServerInternalReply(this.proposerRequest.localAcceptedCommand()), shardStoreServer);
                        this.proposerRequest = null;
                        this.alreadyAcceptedFromFollowers = null;
                        this.proposalRepliers.clear();
                    }
                    else {
                        // set clientCommand as localAcceptedCommand, and do phase-2 broadcast
                        this.proposerRequest = new ProposerRequest(2, new ProposalNum(roundNum, this.serverID), this.proposerRequest.slotNum(), this.proposerRequest.clientCommand(), null);
                        this.proposalRepliers.clear();
                        broadcast(this.proposerRequest);
                    }
                }
            }

        }
    }



    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onHeartbeatTimer(HeartbeatTimer t) {
        // if in election mode, server broadcasts its heartbeat
        if (this.leaderAddress == null) {
            broadcast(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex));
        }

        // else in leader-follower mode
        else {
            // if leader is not active, reset leader info and go to election mode
            if (!this.isLeaderAlive && !t.leaderTolerate()) {
                t.leaderTolerate(true);
            } else if (!this.isLeaderAlive && !t.leaderTolerate()) {
                // reset leader info in state
                this.leaderAddress = null;
                this.leaderID = -1;
                broadcast(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex));
            }
            // else the leader is active, keep in leader-follower mode:
            // only leader broadcasts its heartbeat and followers only send heartbeat to leader
            else {
                t.leaderTolerate(true);
                // if leader, then broadcast heartbeat
                if (this.address().equals(leaderAddress)) {
                    broadcast(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex));
                }
                // else follower, only send heartbeat to leader
                else {
                    send(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex), leaderAddress);
                }
            }
        }

        if (!this.address().equals(this.isLeaderAlive)) {
            this.isLeaderAlive = false;
        }
        // clear last T period leader election data
        // => in a 5-server leader election: consider case in (T-1) period, 1, 2, 3, 4, 5 are active; but in T period, only 1, 2 are active, then because only
        // minority is active, so the 5-server group will not have a leader. So it is necessary to clear last T period's record
        // => not in leader election: operation on this.myVoters doesn't harm
        this.myVoters.clear();
        // same to voteID
        this.voteID = this.serverID;

        // GC
        // check if I have all server's firstUnchosenIndex info
        //        if (this.server_FirstUnchosenIndex.size() == this.servers.length && this.log.size() > 50) {
        //            int minFirstUnchosenIndex = Collections.min(this.server_FirstUnchosenIndex.values());
        //            for (int i = gcPointer; i < minFirstUnchosenIndex; i++) {
        //                log.remove(i);
        //            }
        //            gcPointer = minFirstUnchosenIndex;
        //        }

        set(t, HEARTBEAT_MILLIS);

    }

    private void onProposerTimer(ProposerTimer t) {
        // keep broadcasting proposal request only when three conditions are all met:
        // 1. the leader kept the same, the proposer(i.e., leader) hasn't changed
        // 2. the leader hasn't receive a quorum reply
        // 3. the proposing process hasn't been completed

        if (this.address().equals(this.leaderAddress) && this.proposalRepliers.size() + 1 < quorum && this.proposerRequest != null ) {
            broadcast(this.proposerRequest);
            set(t, PROPOSER_RETRY_MILLIS);
        }
    }


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    /**
     * set this server's ServerID as its index in server[] array
     * @param thisServer
     * @param servers
     * @return
     */
    private int generateServerID(Address thisServer, Address[] servers) {
        int serverID = -1;
        for (int i = 0; i < servers.length; i++) {
            if (thisServer.equals(servers[i])) {
                serverID = i;
                break;
            }
        }
        return serverID;
    }


    /**
     * start leader election & set leaderID and leaderAddress in state
     *
     * action:
     *  before reaching terminal condition, each server broadcasts its heartbeat Message
     *
     * terminal condition:
     * 1. this server (followers) hears higher RoundNumber
     *      --> steps down as acceptor (a higher RoundNumber means a new leader has been elected and the new leader updated RoundNum)
     *          and votes to the voteID it has heard, vote is indicated by serverID
     *      --> keep broadcast its heartbeat until hearing from the leader: the sender of (curRoundNum+1) heartbeat is the leader
     * 2. this server (new leader) has gotten a quorum of different voters who vote it
     *      --> step up as leader
     *      --> update curRoundNum++, broadcast HeartBeat Message
     *
     *
     * Here we only need to consider terminal condition 2, as terminal condition 1 has already handled in handleHeartbeat
     * case1. find it will not be a leader: hear a voteID > itsID, so go to vote for the highestID
     * case2. find it has become a new leader: get quorum vote
     */
    private void leaderElection(Heartbeat hb, Address sender) {
        int voteID = hb.voteID();

        if (voteID > this.serverID) {
            this.voteID = Math.max(this.voteID, voteID);
            return;
        }

        // terminal condition 2: this server has gotten a quorum of different voters who vote it
        if (voteID == this.serverID) {
            this.myVoters.add(sender);
        }
        if (this.myVoters.size() + 1 >= this.quorum) { // +1 means this server votes itself to be leader
            this.roundNum = this.roundNum + 1;
            this.leaderAddress = this.address();
            this.leaderID = this.serverID;
        }

    }

    private void broadcast(Message m) {
        for (Address server : servers) {
            if (!this.address().equals(server)) {
                send(m, server);
            }
        }
    }

    private boolean isInLogToExecute(AMOCommand command) {
        for (Integer logNum : log.keySet()) {
            AMOCommand logNumCommand = log.get(logNum).command;
            if (command.equals(logNumCommand)) return true;
        }
        return false;
    }

    private void leaderSyncWithFollower(Heartbeat followerHb) {
        int followerFirstUnchosenIndex = followerHb.firstUnchosenIndex();
        Address followerAddress = followerHb.senderAddress();

        // update the follwer's firstUnchosenIndex and corresponding log for the index
        // in this.server_FirstUnchosenIndex and this.server_LogForFirstUnchosenIndex
        if (followerFirstUnchosenIndex < this.firstUnchosenIndex && followerFirstUnchosenIndex > this.server_FirstUnchosenIndex.get(followerHb.senderAddress())) {
            this.server_FirstUnchosenIndex.put(followerAddress, followerFirstUnchosenIndex);
            Log logInTheSlot = log.get(followerFirstUnchosenIndex);
            this.server_LogForFirstUnchosenIndex.put(followerAddress, logInTheSlot);
        }
    }

    private void followerSyncWithLeader(Heartbeat leaderHb) {
        // check if I need to update my firstUnchosenIndex and corresponding log slot
        int leaderFirstUnchosenIndex = leaderHb.firstUnchosenIndex();
        if (leaderFirstUnchosenIndex > this.firstUnchosenIndex) {
            if (leaderHb.server_FirstUnchosenIndex().get(this.address()) != this.firstUnchosenIndex) return;
            Log logForSlot = leaderHb.server_LogForFirstUnchosenIndex().get(this.address());
            if (logForSlot == null) return;
            // update my log in the firstUnchosenIndex logslot
            log.put(firstUnchosenIndex, logForSlot);

            AMOCommand command = logForSlot.command;
            handleMessage(new ShardServerInternalReply(command), shardStoreServer);
            // update my FirstUnchosenIndex in this.firstUnchosenIndex and this.server_server_FirstUnchosenIndex
            updateFirstUnchosenIndex();
        }

    }

    private void setLogInPosition(ProposerRequest pReq, PaxosLogSlotStatus status) {
        Log logInSlotNum = new Log(pReq.proposalNum(), pReq.slotNum(), status, pReq.localAcceptedCommand());
        log.put(pReq.slotNum(), logInSlotNum);
    }

    private void updateFirstUnchosenIndex() {
        int i = this.firstUnchosenIndex + 1;
        while (this.log.get(i) != null && this.log.get(i).status != CHOSEN) {
            handleMessage(new ShardServerInternalReply(this.log.get(i).command), shardStoreServer);
            i++;
        }
        this.firstUnchosenIndex = i;
        this.server_FirstUnchosenIndex.put(this.address(), this.firstUnchosenIndex);
    }

    public AMOCommand getLogAMOCommand(int logSlotNum) {
        Log consultResult = log.get(logSlotNum);
        if (consultResult == null) return null;
        return consultResult.command;
    }


    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        Log consultResult = log.get(logSlotNum);
        if (consultResult != null) return consultResult.status;
        else {
            return logSlotNum < gcPointer? CLEARED : PaxosLogSlotStatus.EMPTY;
        }
    }

}
