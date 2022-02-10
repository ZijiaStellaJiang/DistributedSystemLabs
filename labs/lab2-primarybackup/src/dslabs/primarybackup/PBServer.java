package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.ForwardTimer.FORWARD_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.RequestAppTimer.REQUESTAPP_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;
    // Your code here...
    private AMOApplication<Application> app;
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
        send(new GetView(), viewServer);
        set(new PingTimer(), PING_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender)
            throws InterruptedException {
        // Your code here...
        if(state == 1){
            // if is primary and has backup and has sent forwardRequest
            // then won't process the request unless the previous gets executed by backup
            while(currentView.backup() != null && forwardRequest != null && forwardReply == null){
                wait();
            }
            AMOResult r = app.execute(m.command());
            send(new Reply(r), sender);
            if(currentView.backup() != null){
                forwardRequest = new ForwardRequest(m.command());
                send(forwardRequest, currentView.backup());
                set(new ForwardTimer(m.command()),FORWARD_MILLIS);
            }
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        View view = m.view();
        if(sender.equals(viewServer) && view.viewNum() > currentView.viewNum()){
            if(this.address().equals(view.primary())){
                if(state == 2 && view.viewNum() == currentView.viewNum() + 1){
                    setAsPrimary(view);
                }
            }else if(this.address().equals(view.backup())){
                setAsBackup(view);
            }
        }
    }

    // Your code here...

    private void handleForwardRequest(ForwardRequest f, Address sender)
            throws InterruptedException {
        if(state == 2 && sender.equals(currentView.primary())){
            while(app == null){
                wait();
            }
            AMOResult r = app.execute(f.command());
            send(new ForwardReply(r), sender);
        }
    }

    private void handleForwardReply(ForwardReply f, Address sender){
        if(state == 1 && sender.equals(currentView.backup())){
            if(f.result().sequenceNum() == forwardRequest.command().sequenceNum()){
                forwardReply = f;
                notify();
            }
        }
    }

    private void handleRequestApp(RequestApp m, Address sender){
        if(state == 1 && sender.equals(currentView.backup())){
            if(m.viewNum() == currentView.viewNum()){
                AppReply r = new AppReply(app);
                send(r,sender);
            }
        }
    }

    private void handleAppReply(AppReply r, Address sender){
        if(state == 2 && sender.equals(currentView.primary())){
            app = r.app();
            notify();
        }
    }
    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // Your code here...
        send(new Ping(currentView.viewNum()), viewServer);
        set(t,PING_MILLIS);
    }


    // Your code here...
    private void onForwardTimer(ForwardTimer t){
        if(t.command().equals(forwardRequest.command()) && forwardReply == null){
            send(new GetView(), viewServer);
            send(forwardRequest, currentView.backup());
        }
        set(t,FORWARD_MILLIS);
    }

    private void onRequestAppTimer(RequestAppTimer t){
        if(t.request().viewNum() == currentView.viewNum()){
            send(new GetView(), viewServer);
            send(t.request(), currentView.primary());
        }
        set(t, REQUESTAPP_MILLIS);
    }
    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private void setAsPrimary(View view){
        currentView = view;
        state = 1;
        forwardRequest = null;
        forwardReply = null;
    }

    private void setAsBackup(View view){
        currentView = view;
        state = 2;
        forwardRequest = null;
        forwardReply = null;
        app = null;
        RequestApp r = new RequestApp(view.viewNum());
        send(r, view.primary());
        set(new RequestAppTimer(r), REQUESTAPP_MILLIS);
    }
}
