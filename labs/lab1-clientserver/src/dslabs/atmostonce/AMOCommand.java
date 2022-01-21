package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
    // Your code here...
    public final Command command;
    public final int sequenceNum;
    public final Address sender;
}
