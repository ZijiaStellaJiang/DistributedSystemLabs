package dslabs.primarybackup;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.kvstore.KVStore;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private View currentView;
    private int curViewNum;
    private AMOApplication app;
    private int state; // 0: idle, 1: primary, 2: backup
    private boolean hasBackupExecuted;
    private boolean isBackupSet;
    private Request currentClientRequest;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        if (app instanceof KVStore) {
            this.app = new AMOApplication((KVStore) app);
        } else {
            this.app = new AMOApplication(new KVStore());
        }

    }

    @Override
    public void init() {
        // Your code here...
        this.currentView = new View(STARTUP_VIEWNUM, null, null);
        this.curViewNum = STARTUP_VIEWNUM;
        this.state = 0;
        this.hasBackupExecuted = false;
        this.isBackupSet =false;
        this.ping();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender)
            throws InterruptedException {
        // Your code here...
        this.currentClientRequest = m;
        // if the server is not primary, then drop message
        if (this.state != 1) {
            return;
        }
        
        // execute
        AMOResult result = app.execute(m.command());
        // if there's no backup, send result to client directly
        if (this.currentView.backup() == null) {
            send(new Reply(result), sender);
        }

        else {
            // if there's a backup, then the primary send the request to the backup
            primarySendRequestToBackup(m);
            
            // send the result to client only when backup has done (or there's no backup)
            while (!this.hasBackupExecuted) {
                wait();
            }
            send(new Reply(result), sender);
        }
    }

    private void handleViewReply(ViewReply m, Address sender)
            throws InterruptedException {
        // Your code here...
        if (m.view().viewNum() > this.currentView.viewNum()) {
            this.currentView = m.view();

            // [primary changed] if this server is pre-backup and now has become primary
            if (this.state == 2 && Objects.equal(m.view().primary(), this.address())) {
                this.state = 1;
            }

            // [backup changed] if this server is primary (still primary) and backup has been updated
            if (this.state == 1 && Objects.equal(m.view().primary(), this.address())) {
                if (m.view().backup() != null) {
                    send(new ReplyRecordCopy(this.app), m.view().backup());
                    while(!this.isBackupSet) {
                        wait();
                    }
                }
            }

            // update state
            if (this.currentView.primary() == this.address()) {
                this.state = 1;
            } else if (this.currentView.backup() == this.address()) {
                this.state = 2;
            } else {
                this.state = 0;
            }
            
        }
    }

    // Your code here...
    
    private synchronized void handleBackupSetup(ViewReply m, Address sender) {
        if (Objects.equal(sender, this.currentView.backup()) && Objects.equal(this.currentView, m)) {
            this.isBackupSet = true;
            notify();
        }
    }

    private void primarySendRequestToBackup(Request m) {
        if (this.currentView.backup() != null) {
            send(m, this.currentView.backup());
        }
    }
    
    private void backupHandleRequest(Request m, Address sender) {
        if (Objects.equal(this.currentView.primary(), sender)) {
            return;
        }
        AMOResult result = app.execute(m.command());
        send(new Reply(result), sender);
    }
    

    private synchronized void primaryHandleResultFromBackup(Reply m, Address sender) {
        if (Objects.equal(this.currentView.backup(), sender) 
                && m.result().sequenceNum() == this.currentClientRequest.command().sequenceNum()) 
        {
            this.hasBackupExecuted = true;
            notify();
        }
    }
    
    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // Your code here...
        //
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private void ping() {
        send(new Ping(curViewNum), viewServer);
    }
}
