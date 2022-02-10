package dslabs.primarybackup;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private Address primaryServer;
    private Command command;
    private Result result;
    private View currentView;
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
        send(new GetView(), this.viewServer);
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @SneakyThrows
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        while (this.primaryServer == null) {
            wait();
        }
        this.command = command;
        this.result = null;
        this.sequenceNum = this.sequenceNum < Integer.MAX_VALUE? this.sequenceNum + 1 : 0;

        send(new Request(new AMOCommand(this.command, sequenceNum, this.address())), this.primaryServer);
        set(new ClientTimer(this.command, sequenceNum), CLIENT_RETRY_MILLIS);

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
        if (m.result().sequenceNum() == this.sequenceNum) {
            result = m.result().result;
            notify();
        }

    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (m.view().viewNum() > this.currentView.viewNum()) {
            this.currentView = m.view();
            this.primaryServer = this.currentView.primary();
            notify();
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (Objects.equal(this.command, t.command()) && this.sequenceNum == t.sequenceNum() && result == null) {
            send(new GetView(), this.viewServer);
            send(new Request(new AMOCommand(this.command, sequenceNum, this.address())), this.primaryServer);
            set(t, CLIENT_RETRY_MILLIS);
        }

    }
}
