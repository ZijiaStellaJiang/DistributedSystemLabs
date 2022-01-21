package dslabs.atmostonce;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.Objects;
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
    private AMOCommand lastAmoCommand;
    private AMOResult lastAmoResult;

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        if (!alreadyExecuted(amoCommand)) {
           Result r = application.execute(amoCommand.command());
           this.lastAmoCommand = amoCommand;
           this.lastAmoResult = new AMOResult(r, amoCommand.sequenceNum());
        }

        return lastAmoResult;
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
        return this.lastAmoCommand == null || amoCommand.sequenceNum() <= lastAmoCommand.sequenceNum();
    }
}
