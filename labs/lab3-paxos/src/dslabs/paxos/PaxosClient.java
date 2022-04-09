package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    // Your code here...
    private Command command;
    private Result result;
    private int sequenceNum = -1;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // Your code here...
        this.command = operation;
        this.result = null;
        sequenceNum++;
        for(Address paxosServer : servers){
            send(new PaxosRequest(new AMOCommand(command, sequenceNum, this.address())), paxosServer);
        }
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
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // Your code here...
        for(Address server: servers){
            if(sender.equals(server) && m.amoResult().sequenceNum() == sequenceNum){
                result = m.amoResult().result();
                notify();
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (Objects.equal(command, t.command()) && result == null) {
            for(Address paxosServer : servers){
                send(new PaxosRequest(new AMOCommand(command, sequenceNum, this.address())), paxosServer);
            }
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
