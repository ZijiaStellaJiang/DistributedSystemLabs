package dslabs.atmostonce;

import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    // Your code here...
    public  final Result command;
    public final int sequenceNum;
}
