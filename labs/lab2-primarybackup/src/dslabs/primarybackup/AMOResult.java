package dslabs.primarybackup;

import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    // Your code here...
    public  final Result result;
    public final int sequenceNum;
}
