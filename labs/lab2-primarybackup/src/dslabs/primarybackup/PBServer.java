package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;
    // Your code here...
    private final AMOApplication<Application> app;
    private View currentView;
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
    private void handleRequest(Request m, Address sender) {
        // Your code here...
        if(state == 1){
            AMOResult r = app.execute(m.command());
            send(new Reply(r), sender);
            //send(new ForwardRequest(m.command()), currentView.backup());
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

    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
