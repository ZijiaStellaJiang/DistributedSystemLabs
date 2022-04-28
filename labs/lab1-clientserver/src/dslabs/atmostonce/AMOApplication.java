package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Your code here...

    Map<Address, Integer> addressLastSeqNumMap = new HashMap<>();
    Map<Address, AMOResult> addressLastAmoResultMap = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        Address sender = amoCommand.sender();
        // Your code here...

        // for handle lab4 Query non-AMO command
        /*
           details: lab4 part2 shardMaster is encapsulated as the app in PaxosServer to handle Query requests from ShardClient and ShardServer;
           and we don't want to add sequence number on Query requests.
         */
        if (amoCommand.sequenceNum == -4) {
            Result r = application.execute(amoCommand.command());
            return new AMOResult(r, amoCommand.sequenceNum());
        }

        // for lab1-3 AMO command
        else if (!alreadyExecuted(amoCommand)) {
            Result r = application.execute(amoCommand.command());
            addressLastSeqNumMap.put(sender, amoCommand.sequenceNum());
            addressLastAmoResultMap.put(sender, new AMOResult(r, amoCommand.sequenceNum()));
        }

        return addressLastAmoResultMap.get(sender);
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand amoCommand) {
        // Your code here...
        Address sender = amoCommand.sender();
        int sequenceNum = amoCommand.sequenceNum();
        return addressLastSeqNumMap.containsKey(sender) &&
                sequenceNum <= addressLastSeqNumMap.get(sender);
    }
}
