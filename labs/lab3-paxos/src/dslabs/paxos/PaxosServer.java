package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;
    // Your code here...
    private final int quorum;
    private AMOApplication<Application> app;
    private int curSlotNum;
    private int firstUnchosenIndex;
    private Map<Integer, Log> log;
    /* note: a Proposal Number = RoundNum + ServerID */
    /* current Round Number of this server */
    private int roundNum;
    /* serverID of this server, which is this server's index in servers[] */
    private int serverID;
    private Address leaderAddress;
    private int leaderID;
    private boolean isLeaderAlive;
    private Set<Integer> smallerServerIDs;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.quorum = servers.length / 2 + 1;
        this.app = new AMOApplication<>(app);
        this.curSlotNum = 1;
        this.firstUnchosenIndex = 1;
        this.log = new HashMap<>();
        this.roundNum = 0;
        this.serverID = generateServerID(address, servers);
        this.leaderAddress = null;
        this.leaderID = -1;
        this.isLeaderAlive = false;
        this.smallerServerIDs = new HashSet<>();
    }


    @Override
    public void init() {
        // Your code here...
        send(new Heartbeat(this.roundNum, this.serverID, this.firstUnchosenIndex), this.address());
        set(new HeartbeatTimer(), HEARTBEAT_MILLIS);
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
        return null;
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
        return null;
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
        return 1;
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
        return 0;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
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
    private void handleHeartBeat(Heartbeat hb, Address sender) {

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

            // info will automatically synchronized with new leader as it is going to communicate with new leader via heartbeat
            // with the update state, this sever will send its heartbeat to new leader
        }


        // message with the same RoundNum as the receiver
        // If the receiver is in leader-election mode
        if (this.leaderAddress == null) {
            leaderElection(hb);
            return;
        }

        // Else, the receiver is working in normal leader-follower mode
        else {
            // case1: this server (i.e., receiver) is the leader
            if (this.address().equals(this.leaderAddress)) {
                // leader also need to update its status (it is itself leader)
                this.isLeaderAlive = true;
                // help follower update firstUnchosenIndex
                leaderSyncWithFollower();
            }

            // case2: this server (i.e., receiver) is a follower, it will only process message from leader in this round's normal cases
            else {
                // update leader alive status
                this.isLeaderAlive = true;
                followerSyncWithLeader();
            }
        }

    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatTimer(HeartbeatTimer t) {
        // if leader is not active
        if (!this.isLeaderAlive) {
            // reset leader info in state
            this.leaderAddress = null;
            this.leaderID = -1;

        } else {

        }

        this.isLeaderAlive = false;
        // clear last T period leader election data
        // => in a 5-server leader election: consider case in (T-1) period, 1, 2, 3, 4, 5 are active; but in T period, only 1, 2 are active, then because only
        // minority is active, so the 5-server group will not have a leader. So it is necessary to clear last T period's record
        // => not in leader election: operation on this.smallerServerIDs doesn't harm
        this.smallerServerIDs.clear();
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
     * 1. this server hears higher RoundNumber
     *      --> step down as acceptor (a higher RoundNumber means a new leader has been elected and the new leader updated RoundNum)
     *      --> keep broadcast its heartbeat until hearing from the leader: the sender of (curRoundNum+1) heartbeat is the leader
     * 2. this server has gotten a quorum of different smaller serverIDs
     *      --> step up as leader
     *      --> update curRoundNum++, broadcast HeartBeat Message
     */
    private void leaderElection(Heartbeat hb) {
        // terminal conditions
        int senderID = hb.senderID();
        // terminal condition 1: this server hears higher RoundNumber --> which has handled in handleHeartbeat, so here we focus on terminal condition 2
        // terminal condition 2: this server has gotten a quorum of different smaller serverIDs
        if (senderID < this.serverID) {
            this.smallerServerIDs.add(senderID);
        }
        if (this.smallerServerIDs.size() + 1 >= this.quorum) { // +1 means this server votes itself to be leader
            this.roundNum = this.roundNum + 1;
            this.leaderAddress = this.address();
            this.leaderID = this.serverID;
        }

    }

    private void leaderSyncWithFollower() {

    }

    private void followerSyncWithLeader() {

    }

}
