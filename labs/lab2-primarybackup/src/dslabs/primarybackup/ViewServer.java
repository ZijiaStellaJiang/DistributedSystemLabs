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
    static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private View currentView;
    private View promotingView;
    boolean acknowledged;
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
        this.promotingView = null;
        this.currentRoundServers = new HashSet<>();
        this.acknowledged = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        currentRoundServers.add(sender);
        if(waitingFirstPrimary()){
            // set the first pinging server as primary directly
            promotingView = new View(INITIAL_VIEWNUM, sender, null);
        }
        if(promotingView != null){
            // try ack
            int viewNum = m.viewNum();
            updateAck(viewNum,sender);
        }
        if(acknowledged){
            // try select backup server
            // as soon as current view is acknowledged,
            // and the current backup is null
            if(waitingBackup()){
                Address backup = selectNewBackup();
                if(backup != null){
                    // only set backup if there is an available one
                    promotingView = new View(currentView.viewNum()+1, currentView.primary(), backup);
                    acknowledged = false;
                }
            }
        }

        View reply = promotingView == null ? currentView : promotingView;
        send(new ViewReply(reply), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        View reply = promotingView == null ? currentView : promotingView;
        send(new ViewReply(reply), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        if(acknowledged){
            // only if the current view is acknowledged
            // there is chance that it can promote to the next view
            if(primaryFail()) {
                promoteBackup();
            }else if(backupFail()){
                // if the backup server fails
                // either find a sub or remove it
                Address backup = selectNewBackup();
                promotingView = new View(currentView.viewNum()+1, currentView.primary(), backup);
                acknowledged = false;
            }
        }
        currentRoundServers = new HashSet<>();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    /**
     * ack
     */
    private void updateAck(int viewNum, Address sender){
        if (sender.equals(promotingView.primary()) && viewNum == promotingView.viewNum()) {
            acknowledged = true;
            currentView = promotingView;
            promotingView = null;
        }
    }

    /**
     * if the backup is null, do nothing
     * else, promote the backup to be primary
     * meanwhile, try to find sub for backup
     */
    private void promoteBackup(){
        Address newPrimary = currentView.backup();
        if(newPrimary!=null){
            Address newBackup = selectNewBackup();
            promotingView = new View(currentView.viewNum()+1, newPrimary, newBackup);
            acknowledged = false;
        }
    }

    /**
     * find a potential backup server
     * from all pinging servers
     * @return server address or null if no such server found
     */
    private Address selectNewBackup(){
        for(Address potentialBackup : currentRoundServers){
            if(!potentialBackup.equals(currentView.primary())
                    && !potentialBackup.equals(currentView.backup())){
                return potentialBackup;
            }
        }
        return null;
    }

    private boolean primaryFail() {
        return currentView.primary() != null && !currentRoundServers.contains(currentView.primary());
    }

    private boolean backupFail() {
        return currentView.backup() != null && !currentRoundServers.contains(currentView.backup());
    }

    private boolean waitingFirstPrimary(){
        return promotingView == null && currentView.viewNum() == STARTUP_VIEWNUM;
    }

    private boolean waitingBackup(){
        return currentView.backup() == null;
    }
}
