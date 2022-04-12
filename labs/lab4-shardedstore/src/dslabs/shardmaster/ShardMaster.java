package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // Your code here...
    private int nextConfigNum = INITIAL_CONFIG_NUM;
    private final Map<Integer, ShardConfig> configurationRecords = new HashMap<>();

    public ShardMaster(int numShards) {
        this.numShards = numShards;
    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        private final int groupId;
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    }


    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // Your code here...
            return processJoin(join);
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // Your code here...
            return processLeave(leave);
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // Your code here...
            return processMove(move);
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // Your code here...
            int queryConfigNum = query.configNum();
            if (this.nextConfigNum == INITIAL_CONFIG_NUM) {
                return new Error();
            } else if (queryConfigNum == -1 || queryConfigNum >= nextConfigNum) {
                return configurationRecords.get(nextConfigNum - 1);
            } else {
                return configurationRecords.get(queryConfigNum);
            }
        }

        throw new IllegalArgumentException();
    }

    private Result processJoin(Join j) {
        int groundId = j.groupId;
        Set<Address> servers = j.servers;
        Set<Integer> shards = new HashSet<>();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
        if (nextConfigNum == INITIAL_CONFIG_NUM) {
            for (int i = 1; i <= numShards; i++) {
                shards.add(i);
            }
            groupInfo.put(groundId, Pair.of(servers, shards));
            configurationRecords.put(nextConfigNum, new ShardConfig(nextConfigNum, groupInfo));
            nextConfigNum++;
            return new Ok();
        }
        else if (configurationRecords.get(nextConfigNum-1).groupInfo.containsKey(groundId)){
            return new Error();
        } else {
            rebalance(groundId, servers);
            nextConfigNum++;
            return new Ok();
        }
    }

    private Result processLeave(Leave l) {
        int groundId = l.groupId;
        if (nextConfigNum == INITIAL_CONFIG_NUM || !configurationRecords.get(nextConfigNum-1).groupInfo.containsKey(groundId)) {
            return new Error();
        } else {
            rebalance(groundId, null);
            nextConfigNum++;
            return new Ok();
        }
    }


    private void rebalance(int groupId, Set<Address> servers) {
        int curGroupNum = configurationRecords.get(nextConfigNum - 1).groupInfo().size();
        int newGroupNum = servers == null? curGroupNum - 1 : curGroupNum + 1;

        // expected re-balanced assignment
        int expectedSmallShardsSize = numShards / newGroupNum;
        int expectedSmallShardsCount = newGroupNum - numShards % newGroupNum;
        int expectedLargeShardsSize = numShards % newGroupNum == 0? expectedSmallShardsSize : expectedSmallShardsSize+1;
        int expectedLargeShardsCount = numShards % newGroupNum;


        List<Integer> spareShards = new LinkedList<>();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = deepClone();

        // collect spareShards
        if (servers != null) { // case: join
            int smallShardsCount = expectedSmallShardsCount;
            int largeShardsCount = expectedLargeShardsCount;
            for (Pair<Set<Address>, Set<Integer>> groupShards : nextGroupInfo.values()) {
                if (largeShardsCount == 0 && smallShardsCount == 1) break;

                int shardsSize = groupShards.getRight().size();

                if (largeShardsCount > 0 && shardsSize > expectedLargeShardsSize) {
                    int moveNum = shardsSize - expectedLargeShardsSize;
                    moveShardsFromGroupToSpare(moveNum, groupShards, spareShards);
                    largeShardsCount--;
                }
                else if (smallShardsCount > 1 && shardsSize > expectedSmallShardsSize) {
                    int moveNum = shardsSize - expectedSmallShardsSize;
                    moveShardsFromGroupToSpare(moveNum, groupShards, spareShards);
                    smallShardsCount--;
                }

            }
            nextGroupInfo.put(groupId, Pair.of(servers, new HashSet<>()));
        } else { // case: leave
            Set<Integer> leaveGroupShards = nextGroupInfo.get(groupId).getRight();
            spareShards.addAll(leaveGroupShards);
            nextGroupInfo.remove(groupId);
        }

        // assign spare shards
        assignSpareShards(expectedSmallShardsSize, expectedSmallShardsCount, expectedLargeShardsSize, expectedLargeShardsCount, nextGroupInfo, spareShards);

        ShardConfig nextShareConfig = new ShardConfig(nextConfigNum, nextGroupInfo);
        configurationRecords.put(nextConfigNum, nextShareConfig);
    }

    private void moveShardsFromGroupToSpare(int moveNum, Pair<Set<Address>, Set<Integer>> moveFromGroup, List<Integer> spareShards) {
        Set<Integer> toRemove = new HashSet<>();
        Iterator<Integer> it = moveFromGroup.getRight().iterator();
        while (moveNum > 0 && it.hasNext()) {
            toRemove.add(it.next());
            moveNum--;
        }
        moveFromGroup.getRight().removeAll(toRemove);
        spareShards.addAll(toRemove);
    }

    private void addSpareShardsToGroup(int addNum, Pair<Set<Address>, Set<Integer>> addToGroup, List<Integer> spareShards) {
        while (addNum > 0) {
            addToGroup.getRight().add(spareShards.get(0));
            spareShards.remove(0);
            addNum--;
        }
    }

    private Result processMove(Move m){
        int toMove = m.shardNum();
        int targetGroupId = m.groupId();
        if (this.nextConfigNum == INITIAL_CONFIG_NUM || configurationRecords.get(nextConfigNum - 1).groupInfo().get(targetGroupId) == null || toMove > numShards || toMove < 1) {
            return new Error();
        }
        if (configurationRecords.get(nextConfigNum - 1).groupInfo().get(targetGroupId).getRight().contains(toMove)) {
            return new Error();
        }
        Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = deepClone();

        for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : nextGroupInfo.entrySet()) {
            if (entry.getKey() == targetGroupId) {
                entry.getValue().getRight().add(toMove);
            } else if (entry.getValue().getRight().contains(toMove)) {
                entry.getValue().getRight().remove(toMove);
            }
        }
        ShardConfig newShardConfig = new ShardConfig(nextConfigNum, nextGroupInfo);
        configurationRecords.put(nextConfigNum, newShardConfig);
        nextConfigNum++;
        return new Ok();
    }


    private Map<Integer, Pair<Set<Address>, Set<Integer>>> deepClone() {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> curGroupInfo = configurationRecords.get(nextConfigNum - 1).groupInfo();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = new HashMap<>();

        for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry: curGroupInfo.entrySet()) {
            Set<Address> servers = new HashSet<>(entry.getValue().getLeft());
            Set<Integer> shards = new HashSet<>(entry.getValue().getRight());
            nextGroupInfo.put(entry.getKey(), Pair.of(servers, shards));
        }

        return nextGroupInfo;
    }

    private void assignSpareShards(int expectedSmallShardsSize, int expectedSmallShardsCount,
                                   int expectedLargeShardsSize, int expectedLargeShardsCount,
                                   Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo,
                                   List<Integer> spareShards) {
        int smallShardsCount = 0;
        int largeShardsCount = 0;

        // assign shards to spareShards
        for (Pair<Set<Address>, Set<Integer>> groupShards : nextGroupInfo.values()) {
            if (largeShardsCount == expectedLargeShardsCount && smallShardsCount == expectedSmallShardsCount) break;
            int shardsSize = groupShards.getRight().size();
            if (shardsSize == expectedSmallShardsSize) smallShardsCount++;
            else if (shardsSize == expectedLargeShardsSize) largeShardsCount++;
            else if (shardsSize < expectedSmallShardsSize && smallShardsCount < expectedSmallShardsCount) {
                int addNum = expectedSmallShardsSize - shardsSize;
                addSpareShardsToGroup(addNum, groupShards, spareShards);
                smallShardsCount++;
            } else {
                int addNum = expectedLargeShardsSize - shardsSize;
                addSpareShardsToGroup(addNum, groupShards, spareShards);
                largeShardsCount++;
            }
        }
    }
}
