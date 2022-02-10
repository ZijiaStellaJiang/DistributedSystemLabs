package dslabs.primarybackup;

import dslabs.framework.Command;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
    static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
    static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final Command command;
}

// Your code here...
@Data
final class ForwardTimer implements Timer {
    static final int FORWARD_MILLIS = 25;
    private final AMOCommand command;
}

@Data
final class RequestAppTimer implements Timer {
    static final int REQUESTAPP_MILLIS = 50;
    private final RequestApp request;
}
