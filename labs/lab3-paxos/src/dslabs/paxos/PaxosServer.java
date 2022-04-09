package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.PaxosLogSlotStatus.ACCEPTED;
import static dslabs.paxos.PaxosLogSlotStatus.CHOSEN;
import static dslabs.paxos.PaxosLogSlotStatus.CLEARED;
import static dslabs.paxos.PaxosLogSlotStatus.EMPTY;
import static dslabs.paxos.ProposerTimer.PROPOSER_RETRY_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;
    // Your code here...
    private final int quorum;
    private AMOApplication<Application> app;
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
    private Set<Address> proposalRepliers;


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.quorum = servers.length / 2 + 1;
        this.app = new AMOApplication<>(app);
        this.firstUnchosenIndex = 1;
        this.log = new HashMap<>();
        this.server_FirstUnchosenIndex = new HashMap<>();
        for (Address server: servers) {
            this.server_FirstUnchosenIndex.put(server, 1);
        }

        this.server_LogForFirstUnchosenIndex = new HashMap<>();
        this.roundNum = 0;
        this.serverID = generateServerID(address, servers);
        this.leaderAddress = servers[2];
        this.leaderID = 2;
        this.isLeaderAlive = true;
        this.myVoters = new HashSet<>();
        this.voteID = this.serverID;
        this.gcPointer = 1;
        this.proposerRequest = null;
        this.proposalRepliers = new HashSet<>();
    }


    @Override
    public void init() {
        // Your code here...
        // broadcast heartbeat to all other servers
//        broadcast(new Heartbeat(this.roundNum, this.serverID, this.address(), this.voteID, this.firstUnchosenIndex, this.server_FirstUnchosenIndex, this.server_LogForFirstUnchosenIndex));
//        HeartbeatTimer t = new HeartbeatTimer();
//        t.leaderTolerate(true);
//        set(t, HEARTBEAT_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the server's local log.
     *
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     *
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        Log consultResult = log.get(logSlotNum);
        if (consultResult != null) return consultResult.status;
        else {
            return logSlotNum < gcPointer? CLEARED : EMPTY;
        }
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     *
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     *
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     *
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        // Your code here...
        AMOCommand amoCommand = getLogAMOCommand(logSlotNum);
        if (amoCommand == null) return null;
        return amoCommand.command();
    }

    public AMOCommand getLogAMOCommand(int logSlotNum) {
        Log consultResult = log.get(logSlotNum);
        if (consultResult == null) return null;
        return consultResult.command;
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        // Your code here...
        return gcPointer;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        // Your code here...
        int logSize = log.size();
        if (logSize > 0) {
            Set<Integer> curSlotNums = log.keySet();
            int maxSlotNum = Collections.max(curSlotNums);
            for (int i = maxSlotNum; i >= gcPointer; i--) {
                if (log.get(i) != null) return i;
            }
        }
        return 0;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // only leader will process client's requests
        if (!this.address().equals(this.leaderAddress)) {
            return;
        }

        AMOCommand command = m.command();
        // this client request has been executed already
        if (app.alreadyExecuted(command)) {
            AMOResult r = app.execute(m.command());
            send(new PaxosReply(r), sender);
            return;
        }

        // two cases:
        // 1. the command has been in non-executed log -> don't reply client, the command in log will be later executed
        // 2. otherwise -> if leader is proposing now, ignore; else leader launch a proposal
        if (!isInLogToExecute(command) && this.proposerRequest == null) {
            int slotNum = this.firstUnchosenIndex;
            AMOCommand localAcceptedCommand = getLogAMOCommand(slotNum);
            this.proposerRequest = new ProposerRequest(new ProposalNum(roundNum, this.serverID), slotNum, localAcceptedCommand, command);
            broadcast(this.proposerRequest);
            set(new ProposerTimer(this.proposerRequest), PROPOSER_RETRY_MILLIS);
        }
    }

    // Your code here...

    /**
     * message with a smaller RoundNum: ignore messages with a smaller RoundNum
     *
     * message with a higher RoundNum: get synchronize with new leader when message with a higher RoundNum
     *
     * message with the same RoundNum as this server's RoundNum:
     * => If (this.leader == null), the receiver is in leader election node
     *  works in leader election mode
     *
     *
     * => Else, the receiver knows its leader is alive
     * two cases: receiver is a follower; receiver is a sender
     * case 1: receiver is a follower
     *         (normal) message from its leader in this round --> leader is alive && synchronize firstUnchosenIndex with leader
     *
     * case 2: receiver is the leader, it will only concern two kind of messages received; the sender should be the new leader
     *         (normal) message from its followers in this round --> help the follower update its firstUnchosenIndex
     *
     * @param hb
     * @param sender
     */
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
               AcceptorReply aReply = new AcceptorReply(new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), true, pReq.localAcceptedCommand());
               send(aReply, sender);
           }

           // else check if it has already accepted command in this slotNum
           else {
               PaxosLogSlotStatus status = status(pReq.slotNum());
               if (ACCEPTED.equals(status)) {
                   AMOCommand acceptedCommand = getLogAMOCommand(pReq.slotNum());
                   AcceptorReply aReply = new AcceptorReply(new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), false, acceptedCommand);
                   send(aReply, sender);
               }
               else if (EMPTY.equals(status)) {
                   AcceptorReply aReply = new AcceptorReply(new ProposalNum(roundNum, this.leaderID), pReq.slotNum(), true, null);
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
                && this.proposerRequest != null
        ) {

            // once find an accepted command, I need to change my command to the already accepted command
            AMOCommand alreadyAcceptedFromFollowers = null;
            if (!aReply.acceptProposal()) {
                alreadyAcceptedFromFollowers = aReply.alreadyAcceptedCommand();
            }
            // update received aReply
            proposalRepliers.add(sender);

            // check if reach quorum
            if (proposalRepliers.size() + 1 >= quorum) {
                if (alreadyAcceptedFromFollowers != null) {
                    // I need to accept the alreadyAcceeptedCommand locally updating this.proposerRequest, and then use the new this.proposerRequest to broadcast
                    this.proposerRequest = new ProposerRequest(new ProposalNum(roundNum, this.serverID), this.proposerRequest.slotNum(), alreadyAcceptedFromFollowers, this.proposerRequest.clientCommand());
                    this.proposalRepliers.clear();
                    broadcast(this.proposerRequest);
                }
                else {
                    if (this.proposerRequest.localAcceptedCommand() != null) {
                        // done. I can update the slot status to chosen, and execute it
                        setLogInPosition(this.proposerRequest, CHOSEN);
                        app.execute(this.proposerRequest.localAcceptedCommand());
                        updateFirstUnchosenIndex();
                        this.proposerRequest = null;
                        this.proposalRepliers.clear();
                    }
                    else {
                        // set clientCommand as localAcceptedCommand, and do phase-2 broadcast
                        this.proposerRequest = new ProposerRequest(new ProposalNum(roundNum, this.serverID), this.proposerRequest.slotNum(), this.proposerRequest.clientCommand(), null);
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
    // Your code here...
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
        if (this.server_FirstUnchosenIndex.size() == this.servers.length) {
            int minFirstUnchosenIndex = Collections.min(this.server_FirstUnchosenIndex.values());
            for (int i = gcPointer; i < minFirstUnchosenIndex; i++) {
                log.remove(i);
            }
        }

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
        if (followerFirstUnchosenIndex > this.server_FirstUnchosenIndex.get(followerAddress)) {
            this.server_FirstUnchosenIndex.put(followerAddress, followerFirstUnchosenIndex);
            Log logInTheSlot = log.get(followerFirstUnchosenIndex);
            this.server_LogForFirstUnchosenIndex.put(followerAddress, logInTheSlot);
        }
    }

    private void followerSyncWithLeader(Heartbeat leaderHb) {
        // check if I need to update my firstUnchosenIndex and corresponding log slot
        int leaderFirstUnchosenIndex = leaderHb.firstUnchosenIndex();
        if (leaderFirstUnchosenIndex > this.firstUnchosenIndex) {
            Log logForSlot = leaderHb.server_LogForFirstUnchosenIndex().get(this.address());
            // update my log in the firstUnchosenIndex logslot
            log.put(firstUnchosenIndex, logForSlot);

            Command command = logForSlot.command;
            app.execute(command);
            // update my FirstUnchosenIndex
            updateFirstUnchosenIndex();

            // update my FirstUnchosenIndex in this.server_server_FirstUnchosenIndex
            this.server_FirstUnchosenIndex.put(this.address(), this.firstUnchosenIndex);
        }

    }

    private void setLogInPosition(ProposerRequest pReq, PaxosLogSlotStatus status) {
        Log logInSlotNum = new Log(pReq.proposalNum(), pReq.slotNum(), status, pReq.localAcceptedCommand());
        log.put(pReq.slotNum(), logInSlotNum);
    }

    private void updateFirstUnchosenIndex() {
        int i = this.firstUnchosenIndex + 1;
        while (this.log.get(i) != null && this.log.get(i).status != CHOSEN) {
            i++;
        }
        this.firstUnchosenIndex = i;
        this.server_FirstUnchosenIndex.put(this.address(), this.firstUnchosenIndex);
    }
}
