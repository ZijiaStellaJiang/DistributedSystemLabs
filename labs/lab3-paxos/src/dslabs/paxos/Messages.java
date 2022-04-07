package dslabs.paxos;

import dslabs.framework.Message;
import lombok.Data;

@Data
class ForwardRequest implements Message {
    private final AMOCommand command;
}

@Data
class ForwardReply implements Message{
    private final AMOResult result;
}