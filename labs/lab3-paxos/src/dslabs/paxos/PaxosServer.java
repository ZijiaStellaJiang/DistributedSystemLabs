package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import dslabs.shardkv.PaxosResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;

    // Your code here...
    private final AMOApplication<Application> app;
    private final Address executor;
    private final Map<Integer, PaxosLogSlot> log;
    private final Map<AMOCommand, Integer> commandSlotNumMap;
    private final Set<Address> allServers;
    private final Map<Address, Integer> firstUnchosenSlotNumMap;
    private Address leaderAddress;
    private final int quorum;
    private final PaxosSlotNumPointer slotNumPointer;
    private boolean inElection;
    private int roundNum;
    private int serverId;
    private int state;
    //0: follower, 1: leader

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.app = new AMOApplication<>(app);
        this.executor = null;
        this.log = new HashMap<>();
        this.commandSlotNumMap = new HashMap<>();
        this.firstUnchosenSlotNumMap = new HashMap<>();
        for(Address server : servers){
            firstUnchosenSlotNumMap.put(server, 1);
        }
        this.allServers = Arrays.stream(servers).collect(Collectors.toSet());
        this.roundNum = 0;
        this.serverId = findServerId();
        this.slotNumPointer = new PaxosSlotNumPointer();
        this.state = 0;
        this.quorum = servers.length / 2 + 1;

//        this.leaderElection = true;
        // temporarily make the last one to be the leader,
        // leader election remains to be implemented
        this.leaderAddress = servers[servers.length - 1];
        if (address.equals(servers[servers.length-1])){
            this.state = 1;
        }
    }

    public PaxosServer(Address address, Address[] servers, Address executor){
        super(address);
        this.servers = servers;

        this.app = null;
        this.executor = executor;
        this.log = new HashMap<>();
        this.commandSlotNumMap = new HashMap<>();
        this.firstUnchosenSlotNumMap = new HashMap<>();
        for(Address server : servers){
            firstUnchosenSlotNumMap.put(server, 1);
        }
        this.allServers = Arrays.stream(servers).collect(Collectors.toSet());
        this.roundNum = 0;
        this.serverId = findServerId();
        this.slotNumPointer = new PaxosSlotNumPointer();
        this.state = 0;
        this.quorum = servers.length / 2 + 1;

        //        this.leaderElection = true;
        // temporarily make the last one to be the leader,
        // leader election remains to be implemented
        this.leaderAddress = servers[servers.length - 1];
        if (address.equals(servers[servers.length-1])){
            this.state = 1;
        }
    }

    @Override
    public void init() {
        // Your code here...
        // set heartbeatTimer
        set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        AMOCommand amoCommand = m.amoCommand();
        if (app.alreadyExecuted(amoCommand)){
            // if the command has been executed, return the result
            AMOResult r = app.execute(amoCommand);
            send(new PaxosReply(r), m.amoCommand().sender());
        }else if (state == 0) {
            // if no result && this server is a follower
            // redirect the message to leader
            send(m, leaderAddress);
        }else if (state == 1) {
            // no result, this is leader
            // try propose
            allocateSlot(slotNumPointer.firstEmptySlotNum(), amoCommand);
            updateFirstEmptySlotNum();
        }
    }

    // Your code here...

    private void handleLeaderMessage(LeaderMessage lm, Address sender){
        if (state == 0 && isLeader(sender)){
            executeAndGC(lm);
            updateFollowerLog(lm);
            updateFollowerSlotNumPointer();
        }
    }

    private void handleFollowerMessage(FollowerMessage fm, Address sender){
        if (state == 1 && isFollower(sender)) {
            updateLeaderLog(fm, sender);
            updateExecuteToNum(fm, sender);
        }
    }

    private void handleElectionRequest(ElectionRequest er, Address sender) {
        if (inElection) {

        }
    }

    private void handleElectionResponse(ElectionResponse er, Address sender) {

    }

    private void handleLeaderAnnounce(LeaderAnnounce la, Address sender) {

    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatTimer(HeartbeatTimer t){
        if (state == 1) {
            // self check if any slot should be chosen
            checkQuorum();
            // broadcast
            broadcast(new LeaderMessage(log, slotNumPointer));
        } else if (state == 0) {
            send(new FollowerMessage(log, slotNumPointer), leaderAddress);
        }
        set(t,HeartbeatTimer.HEARTBEAT_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void checkQuorum(){
        for ( int i = slotNumPointer.firstUnchosenSlotNum(); i <=
                slotNumPointer.lastNonEmptySlotNum(); i++){
            if (log.containsKey(i)){
                PaxosLogSlot slot = log.get(i);
                if (slot.status().equals(PaxosLogSlotStatus.ACCEPTED) && slot.acceptors().size() >= quorum){
                    slot.status(PaxosLogSlotStatus.CHOSEN);
                    executeProposal(slot.amoCommand());
                }
            }
        }
    }

    private void updateExecuteToNum(FollowerMessage fm, Address sender){
        PaxosSlotNumPointer followerPointer = fm.slotNumPointer();
        firstUnchosenSlotNumMap.put(sender, followerPointer.firstUnchosenSlotNum());
        int newExecuteToNum = followerPointer.firstUnchosenSlotNum() - 1;
        for (Address server : firstUnchosenSlotNumMap.keySet()){
            newExecuteToNum = Math.min(newExecuteToNum, firstUnchosenSlotNumMap.get(server) - 1);
        }
        if (newExecuteToNum > slotNumPointer.executeToSlotNum()){
            slotNumPointer.executeToSlotNum(newExecuteToNum);
        }
    }

    private void updateLeaderLog(FollowerMessage fm, Address sender){
        Map<Integer, PaxosLogSlot> followerLog = fm.log();
        for(Integer slotNum : followerLog.keySet()){
            if (slotNum >= slotNumPointer.firstUnchosenSlotNum()){
                PaxosLogSlot followerSlot = followerLog.get(slotNum);
                PaxosLogSlot leaderSlot = log.get(slotNum);
                if (leaderSlot.status().equals(PaxosLogSlotStatus.ACCEPTED)){
                    if (followerSlot.compareTo(leaderSlot) < 0){
                        commandSlotNumMap.remove(leaderSlot.amoCommand());
                        followerSlot.acceptors().add(this.address());
                        log.put(slotNum, followerSlot);
                        commandSlotNumMap.put(followerSlot.amoCommand(), slotNum);
                    } else if (followerSlot.compareTo(leaderSlot) == 0){
                        leaderSlot.acceptors().add(sender);
                        if (leaderSlot.acceptors().size() >= quorum){
                            leaderSlot.status(PaxosLogSlotStatus.CHOSEN);
                            executeProposal(leaderSlot.amoCommand());
                        }
                    }
                }
            }
        }
    }

    private void updateFollowerSlotNumPointer(){
        int newFirstUnchosenSlotNum = slotNumPointer.firstUnchosenSlotNum();
        int newFirstEmptySlotNum = slotNumPointer.firstEmptySlotNum();
        while(log.containsKey(newFirstEmptySlotNum)){
            newFirstEmptySlotNum++;
        }
        while(log.containsKey(newFirstUnchosenSlotNum) && log.get(newFirstUnchosenSlotNum).status().equals(PaxosLogSlotStatus.CHOSEN)){
            newFirstUnchosenSlotNum++;
        }
        slotNumPointer.firstUnchosenSlotNum(newFirstUnchosenSlotNum);
        slotNumPointer.firstEmptySlotNum(newFirstEmptySlotNum);
    }

    private void updateFollowerLog(LeaderMessage lm){
        Map<Integer, PaxosLogSlot> leaderLog = lm.log();
        int newLastNonEmptySlotNum = slotNumPointer.lastNonEmptySlotNum();
        for (Integer slotNum : leaderLog.keySet()){
            if (slotNum >= slotNumPointer.firstNonClearedSlotNum()){
                newLastNonEmptySlotNum = Math.max(newLastNonEmptySlotNum, slotNum);
                PaxosLogSlot leaderSlot = leaderLog.get(slotNum);
                PaxosLogSlot followerSlot = log.get(slotNum);
                if (followerSlot == null) {
                    // 1. follower slot is empty
                    leaderSlot.acceptors().add(this.address());
                    log.put(slotNum, leaderSlot);
                    commandSlotNumMap.put(leaderSlot.amoCommand(),slotNum);
                } else if (followerSlot.status().equals(PaxosLogSlotStatus.ACCEPTED)){
                    // 2. follower slot has an accepted value
                    // only sync with leader when leader's is earlier
                    if (leaderSlot.compareTo(followerSlot) < 0){
                        commandSlotNumMap.remove(followerSlot.amoCommand());
                        leaderSlot.acceptors().add(this.address());
                        log.put(slotNum, leaderSlot);
                        commandSlotNumMap.put(leaderSlot.amoCommand(), slotNum);
                    }
                }
            }
        }
        slotNumPointer.lastNonEmptySlotNum(newLastNonEmptySlotNum);
    }

    private void executeAndGC(LeaderMessage lm){
        Map<Integer, PaxosLogSlot> leaderLog = lm.log();
        int executeToSlotNum = lm.slotNumPointer().executeToSlotNum();
        int executeFromSlotNum = slotNumPointer.firstNonClearedSlotNum();
        while(executeFromSlotNum <= executeToSlotNum){
            PaxosLogSlot leaderSlot = leaderLog.get(executeFromSlotNum);
            assert(leaderSlot!=null && leaderSlot.status().equals(PaxosLogSlotStatus.CHOSEN));
            PaxosLogSlot mySlot = log.get(executeFromSlotNum);
            assert(mySlot!=null); // and the status should be either accepted or chosen
            executeProposal(leaderSlot.amoCommand());

            gc(executeFromSlotNum);
            executeFromSlotNum++;
        }
        slotNumPointer.firstNonClearedSlotNum(executeToSlotNum+1);
        slotNumPointer.executeToSlotNum(executeToSlotNum);
    }

    private void executeProposal(AMOCommand c){
        if (app == null) {
            send(new PaxosResult(c), executor);
        } else {
            app.execute(c);
        }
    }

    private void gc(int slotNum){
        PaxosLogSlot slot = log.get(slotNum);
        assert(slot!=null);
        commandSlotNumMap.remove(slot.amoCommand());
        log.remove(slotNum);
    }

    private void broadcast(Message m){
        for(Address paxosServer : servers){
            if(!paxosServer.equals(this.address())){
                send(m, paxosServer);
            }
        }
    }

    private int findServerId(){
        for(int i = 0;i<servers.length;i++){
            if(servers[i].equals(this.address())){
                return i;
            }
        }
        return -1;
    }

    private void allocateSlot(int slotNum, AMOCommand amoCommand){
        if (!commandSlotNumMap.containsKey(amoCommand)){
            commandSlotNumMap.put(amoCommand, slotNum);
            PaxosLogSlot logSlot = new PaxosLogSlot(roundNum, serverId, slotNum, PaxosLogSlotStatus.ACCEPTED, new HashSet<>(), amoCommand);
            logSlot.acceptors().add(this.address());
            log.put(slotNum, logSlot);
            slotNumPointer.lastNonEmptySlotNum(Math.max(slotNumPointer.lastNonEmptySlotNum(), slotNum));
        }
    }

    private void updateFirstEmptySlotNum(){
        while(!status(slotNumPointer.firstEmptySlotNum()).equals(PaxosLogSlotStatus.EMPTY)){
            slotNumPointer.firstEmptySlotNum(slotNumPointer.firstEmptySlotNum() + 1);
        }
    }

    private boolean isLeader(Address address){
        return leaderAddress.equals(address);
    }

    private boolean isFollower(Address address){
        return !leaderAddress.equals(address) && allServers.contains(address);
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
        if (log.containsKey(logSlotNum)){
            return log.get(logSlotNum).status();
        }else if(slotNumPointer.firstEmptySlotNum() > logSlotNum){
            return PaxosLogSlotStatus.CLEARED;
        }
        return PaxosLogSlotStatus.EMPTY;
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
        if (log.containsKey(logSlotNum)){
            return log.get(logSlotNum).amoCommand().command();
        }
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
        return slotNumPointer.firstNonClearedSlotNum();
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
        return slotNumPointer.lastNonEmptySlotNum();
    }
}
