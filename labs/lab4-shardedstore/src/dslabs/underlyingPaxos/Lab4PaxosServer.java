package dslabs.underlyingPaxos;

import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Lab4PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;
    private final Address shardStoreServer;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public Lab4PaxosServer(Address address, Address[] servers, Address shardStoreServer) {
        super(address);
        this.servers = servers;
        this.shardStoreServer = shardStoreServer;

        // Your code here...
    }


    @Override
    public void init() {
        // Your code here...
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(Command command, Address sender) {
        // Your code here...
    }

    // Your code here...


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
