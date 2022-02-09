package dslabs.primarybackup;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private View currentView;
    private Command command;
    private Result result;
    private int sequenceNum = -1;
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // Your code here...
        updateView();
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        this.command = command;
        this.result = null;

        sequenceNum++;

        send(new Request(new AMOCommand(command, sequenceNum, this.address())), currentView.primary());
        set(new ClientTimer(command), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        // Your code here...
        if (m.result().sequenceNum() == sequenceNum) {
            result = m.result().result();
            notify();
        }
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if(sender.equals(viewServer) && m.view().viewNum() >
                currentView.viewNum()){
            currentView = m.view();
        }
    }

    // Your code here...
    private synchronized void updateView(){
        send(new GetView(),viewServer);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t){
        // Your code here...
        if (Objects.equal(command, t.command()) && result == null) {
            updateView();
            send(new Request(new AMOCommand(command, sequenceNum, this.address())), currentView.primary());
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
