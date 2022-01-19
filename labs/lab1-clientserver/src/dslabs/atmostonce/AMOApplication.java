package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.Append;
import dslabs.kvstore.KVStore.Get;
import dslabs.kvstore.KVStore.GetResult;
import dslabs.kvstore.KVStore.Put;
import dslabs.kvstore.KVStore.PutOk;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.checkerframework.checker.units.qual.C;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Your code here...
    /**
     * key: String, sender address
     * value: int, lastTimeExecutedSeqNum
     */
    private ConcurrentHashMap<String, Integer> reqIDRecord
            = new ConcurrentHashMap<>();

    /**
     * key: String, sender address
     * value: int, lastTimeReply
     */
    private ConcurrentHashMap<String, Result> replyCache
            = new ConcurrentHashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        boolean alreadyExecuted = alreadyExecuted(amoCommand);
        if (alreadyExecuted) {
            Result result = replyCache.get(amoCommand.sender.toString());
            return new AMOResult(result, amoCommand.sequenceNum, amoCommand.sender);
        }

        Result result = application.execute(amoCommand.command);
        reqIDRecord.put(amoCommand.sender.toString(), amoCommand.sequenceNum);
        replyCache.put(amoCommand.sender.toString(), result);

        return new AMOResult(result, amoCommand.sequenceNum, amoCommand.sender);
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
        Address sender = amoCommand.sender;
        Integer lastReqID = reqIDRecord.get(sender.toString());
        if (lastReqID == null) return false;
        int curReqID = amoCommand.sequenceNum;
        return curReqID <= lastReqID;
    }
}
