# Paxos
*Adapted from the [MIT 6.824
Labs](http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html)*


## Lab3 Result
* Passed 21/27 test cases
* Failed 6 test cases
  * failed two search test 22, 23
  * timeout in three test cases 7, 17, 18
  * GC test 11: although we tried to GC logs, other storage overhead may also weight.



## Overview
We completed this lab followed lab3 instructions and studied the Paxos Made Moderately Complex paper as well as other references to get a better understanding.

**The key elements in implement design are:**
* leader election
* heartbeart
* leader-follower synchronize on the leader's alive and log
* proposal

**Consider scenarios including:**
* leader fail-over
* network delay or network partition


**Our design:**
* Stable leader: leader only changes if it fails, or it's stale
* Server is stateful in role player: we didn't divide servers to different roles but let each server can play multiple roles.
* Each server maintains its log
* Use ballot (term, logIndex)
* Use heartbeart to synchronize the leader alive and logs between leader and followers
* leader give followers the log info for the follower updates/ sync log in heartbeat
* followers send its first unchosen log to leader, and leader is the center to have an overview of all servers' log, then help the global log synchronization.
* Perform GC when all servers have executed the previous commands in log
* In leader election and proposal, we use majority vote



## Optimizations we did
* Well-designed log data structure and heartbeat info
* Use first unchosen index variable for log synchronization, which is much light-weighted than sending the whole log, and thus saved network communication load & time.
* Frugality in leader election, we tried to achieve less frequent leader election by avoid unnecessary leader election.
* Add edge case for only one server in the network, under which scenario just simple node working mechanism is enough and save time.


## Future Work
Overall, the main challenge we face now is timeout and GC.
* For timeout, when network partition, our design tends to timeout. So we need to look into more time-saving strategies in both leader-election as well as proposal.
* For GC, we need to reconsider the current design and find out unnecessary data storage and thus give a more efficient design. On the other hand, we can consider if there are other places where we can also apply GC; currently, we only apply GC on log of servers.

---

## Reference
* [Paxos Made Moderately Complex](https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf)
* CPS 512 slides Paxos; Paxos  2PC
* [Implementing Replicated Logs with Paxos -- Stanford University Slide](https://ongardie.net/static/raft/userstudy/paxos.pdf)
* [Understanding Paxos](https://people.cs.rutgers.edu/~pxk/417/notes/paxos.html)
* [Paxos wikipedia](https://en.wikipedia.org/wiki/Paxos_(computer_science))
