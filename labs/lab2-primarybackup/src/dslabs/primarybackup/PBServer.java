package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.ForwardTimer.FORWARD_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;
    // Your code here...
    private final AMOApplication<Application> app;
    private View currentView;
    private ForwardRequest forwardRequest;
    private ForwardReply forwardReply;
    // 0: idle, 1: primary, 2: backup
    private int state;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.app = new AMOApplication<>(app);
        this.viewServer = viewServer;
        this.state = 0;
        // Your code here...
    }

    @Override
    public void init() {
        // Your code here...
        this.currentView = new View(STARTUP_VIEWNUM,null,null);
        set(new PingTimer(), PING_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender)
            throws InterruptedException {
        // Your code here...
        if(state == 1){
            //TODO: forwardReply == null 这个条件有很大问题
            while(currentView.backup() != null && forwardReply == null){
                wait();
            }
            AMOResult r = app.execute(m.command());
            send(new Reply(r), sender);
            if(currentView.backup() != null){
                send(new ForwardRequest(m.command()), currentView.backup());
                set(new ForwardTimer(m.command()),FORWARD_MILLIS);
            }
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if(sender.equals(viewServer) && m.view().viewNum() > currentView.viewNum()){
            if(m.view().viewNum() == currentView.viewNum()+1){
                if(state==0){

                }
            }else{

            }
        }
    }

    // Your code here...

    private void handleForwardRequest(ForwardRequest f, Address sender){
        if(state == 2 && sender.equals(currentView.primary())){
            AMOResult r = app.execute(f.command());
            send(new ForwardReply(r), sender);
        }
    }

    private void handleForwardReply(ForwardReply f, Address sender){
        if(state == 1 && sender.equals(currentView.backup())){
            if(f.result().sequenceNum == forwardRequest.command().sequenceNum){
                forwardReply = f;
                notify();
            }
        }
    }

    private void handleRequestApp(RequestApp m, Address sender){

    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // Your code here...
        send(new Ping(currentView.viewNum()), viewServer);
    }

    private void onForwardTimer(ForwardTimer t){
        send(new ForwardRequest(t.command()), currentView.backup());
        set(t,FORWARD_MILLIS);
    }
    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
