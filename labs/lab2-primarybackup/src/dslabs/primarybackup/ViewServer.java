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
    private View currentView;
    private View promotingView;
    boolean acknowledged;
    Set<Address> lastRoundServers;
    Set<Address> currentRoundServers;
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
        this.currentView = new View(STARTUP_VIEWNUM,null,null);
        this.promotingView = new View(STARTUP_VIEWNUM,null,null);
        this.lastRoundServers = new HashSet<>();
        this.currentRoundServers = new HashSet<>();
        this.acknowledged = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        int viewNum = m.viewNum();
        if(!acknowledged){
            updateAck(viewNum,sender);
        }
        currentRoundServers.add(sender);
        send(new ViewReply(this.currentView),sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        send(new ViewReply(this.currentView),sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        if (getCurrentViewNum() == STARTUP_VIEWNUM) {

        }
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void updateAck(int viewNum, Address sender){
        if (sender.equals(getPromotingPrimary()) && viewNum == getPromotingViewNum()) {
            this.acknowledged = true;
            this.currentView = this.promotingView;
            this.promotingView = null;
        }
    }

    private int getCurrentViewNum() {
        return this.currentView.viewNum();
    }

    private Address getCurrentPrimary() {
        return this.currentView.primary();
    }

    private Address getCurrentBackup() {
        return this.currentView.backup();
    }

    private int getPromotingViewNum() {
        return this.promotingView.viewNum();
    }

    private Address getPromotingPrimary() {
        return this.promotingView.primary();
    }

    private Address getPromotingBackup() {
        return this.promotingView.backup();
    }



    private boolean primaryFail() {
        return !currentRoundServers.contains(getCurrentPrimary());
    }

    private boolean backupFail() {
        return !currentRoundServers.contains(getCurrentBackup());
    }
}
