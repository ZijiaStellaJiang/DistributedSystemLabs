package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashSet;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private int current_viewNum;
    private View curView;
    private View nextView;
    private Address primary;
    private Address backup;
    private Address nextPrimary;
    private Address nextBackup;
    private int timerOn1or2;
    private boolean primaryAck;
    private boolean viewChanged;
    private Set<Address> serversPing1;
    private Set<Address> serversPing2;
//    private ConcurrentHashMap<Address, Boolean> serversAckMapping1;
//    private ConcurrentHashMap<Address, Boolean> serversAckMapping2;
    private Set<Address> currentRoundServers;
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
        // Your code here...
        this.current_viewNum = STARTUP_VIEWNUM;
        this.curView = new View(STARTUP_VIEWNUM, null, null);
        this.primary = null;
        this.backup = null;
        this.nextPrimary = null;
        this.nextBackup = null;
        this.primaryAck = false;
        this.viewChanged = false;
        this.serversPing1 = new HashSet<>();
        this.serversPing2 = new HashSet<>();
//        this.serversAckMapping1= new ConcurrentHashMap<>();
//        this.serversAckMapping2= new ConcurrentHashMap<>();
        this.currentRoundServers = new HashSet<>();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        currentRoundServers.add(sender);

        if (sender.equals(this.primary)) {
            if (m.viewNum() == this.current_viewNum) {
                this.primaryAck = true;
            } else {
                this.primaryAck = false;
            }
        }

        // prepare nextView
        // starting stage
        // if view(0): primary is null, backup is null --> set up primary as the first received ping
        // set the first received ping's sender as primary, and this will be the initial_view view(1)
        if (current_viewNum == STARTUP_VIEWNUM) {
            this.nextPrimary = sender;
            this.nextView = new View(INITIAL_VIEWNUM, this.nextPrimary, null);
        }

        // case1: if view(1): primary is not null, backup is null --> setup backup from server_set
        // or
        // case2: if primary is active and backup is null (waiting for a backup) --> set up backup as the first received ping from an idle server
//        else if (current_viewNum == INITIAL_VIEWNUM) {
//            if (!sender.equals(this.primary) && this.nextBackup == null) {
//                this.nextBackup = selectOneServer();
//                this.nextView = new View(INITIAL_VIEWNUM+1, this.primary, this.nextBackup);
//            }
//        }

        // if primary is active and backup is null (waiting for a backup) --> set up backup as the first received ping from an idle server
        else if (this.primary != null && this.backup == null) {
            if (!sender.equals(this.primary) && this.nextBackup == null) {
                this.nextBackup = selectOneServer();
                this.nextView = new View(this.current_viewNum+1, this.primary, this.nextBackup);
            }
        }


        if (nextView != null) {
            if (    this.current_viewNum == STARTUP_VIEWNUM ||
                    this.primaryAck
            )
            {
                this.updateView();
            }
        }

        View reply = this.nextView == null ? this.curView : this.nextView;
        send(new ViewReply(reply), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        // simply return the currentView to client
        send(new ViewReply(this.curView), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        if (this.primaryAck) {
            // primary failed
            if (!this.currentRoundServers.contains(this.primary)) {
                // update view: if backup is alive, promote backup as the new primary
                this.setNewPrimary();
            }
            // else if backup failed
            else if (this.backup != null && !this.currentRoundServers.contains(this.backup)) {
                // update view: select an idle server as new backup
                this.setNewBackup();
            }
        }
//        else {
//            if (!this.currentRoundServers.contains(this.backup)) {
//                this.nextBackup = selectOneServer();
//            }
//        }
        // check if idle server which has been set as nextBackup failed
        if (this.nextBackup != null && !this.currentRoundServers.contains(this.nextBackup)) {
            this.nextBackup = null;
            this.nextView = null;
        }


        currentRoundServers.clear();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    /**
     * find a potential backup server
     * from all pinging servers
     * @return server address or null if no such server found
     */
    private Address selectOneServer(){
        for(Address newServerForWork : currentRoundServers){
            if(!newServerForWork.equals(this.primary)
                    && !newServerForWork.equals(this.backup)){
                return newServerForWork;
            }
        }
        return null;
    }

    /**
     * update current view to next view
     * reset nextXXX
     */
    private void updateView() {
        // update view
        if (this.nextView != null) {
            this.curView = this.nextView;
            this.current_viewNum += 1;
            this.primary = this.curView.primary();
            this.backup = this.curView.backup();

            this.nextView = null;
            this.nextPrimary = null;
            this.nextBackup = null;
            this.primaryAck = false;
        }
    }

    /**
     * if backup is alive, promote backup as the new primary and select an idle server as new backup
     * else (backup is null or backup is also die) do nothing?
     */
    private void setNewPrimary() {
        if (this.backup != null && currentRoundServers.contains(this.backup)) {
            this.nextPrimary = this.backup;
            this.nextBackup = selectOneServer();
            this.nextView = new View(this.current_viewNum+1, this.nextPrimary, this.nextBackup);
            this.updateView();
        }

    }

    /**
     *
     */
    private void setNewBackup() {
        this.nextBackup = selectOneServer();
        this.nextView = new View(this.current_viewNum+1, this.primary, this.nextBackup);
        this.updateView();
    }

}
