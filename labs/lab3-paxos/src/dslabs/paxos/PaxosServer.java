package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;

    // Your code here...
    private final AMOApplication<Application> app;
    private final Map<Integer, PaxosLogSlot> log;
    private final Map<AMOCommand, Integer> commandSlotNumMap;
    private final Map<AMOCommand, AMOResult> commandResultMap;
    private final int serverId;
    private int round;
    private Address leaderAddress;
    private int quorum;
    private int firstNonCleared;
    private int lastNonEmpty;
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
        this.log = new HashMap<>();
        this.commandSlotNumMap = new HashMap<>();
        this.commandResultMap = new HashMap<>();
        this.firstNonCleared = 1;
        this.lastNonEmpty = 0;
        this.state = 0;
        this.quorum = servers.length / 2 + 1;

        this.serverId = findServerId();

        // temporarily make the last one to be the leader,
        // leader election remains to be implemented
        this.round = 0;
        this.leaderAddress = servers[servers.length - 1];
        if (address.equals(servers[servers.length-1])){
            this.state = 1;
        }
    }


    @Override
    public void init() {
        // Your code here...

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
        }else if(firstNonCleared > logSlotNum){
            return PaxosLogSlotStatus.CLEARED;
        }
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
        return firstNonCleared;
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
        return lastNonEmpty;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        AMOCommand amoCommand = m.amoCommand();
        if (hasResult(amoCommand)) {
            // 如果已经有结果,直接返回结果
            send(new PaxosReply(commandResultMap.get(amoCommand)), sender);
        } else if (state == 0) {
            // 如果没有结果且本机不是leader, 转发给leader决定
            send(new ForwardRequest(m.amoCommand()),leaderAddress);
        } else if (state == 1) {
            // 如果没有分配过slot, 那就检查firstNonCleared开始 第一个空值

        }
    }

    // Your code here...

    private void handleForwardRequest(ForwardRequest fr, Address sender) {
        // Your code here...
        if (state == 1) {

        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
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

    private boolean hasResult(AMOCommand amoCommand){
        return commandResultMap.getOrDefault(amoCommand, null) != null;
    }

}
