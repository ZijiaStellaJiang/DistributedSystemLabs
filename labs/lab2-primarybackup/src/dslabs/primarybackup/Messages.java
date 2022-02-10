package dslabs.primarybackup;

import dslabs.primarybackup.AMOApplication;
import dslabs.framework.Message;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    // Your code here...
    private final AMOCommand command;
}

@Data
class Reply implements Message {
    // Your code here...
    private final AMOResult result;
}

// Your code here...
@Data
class RequestRecordCopy implements Message {
}


@Data
class ReplyRecordCopy implements Message {
    private final AMOApplication app;
}