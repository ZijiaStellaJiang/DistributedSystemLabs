package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.units.qual.A;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // Your code here...
    private int nextConfigNum = INITIAL_CONFIG_NUM;
    private final Map<Integer, ShardConfig> configsMap = new HashMap<>();

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
            return processQuery(query);
        }

        throw new IllegalArgumentException();
    }

    private ShardConfig latestConfig() {
        if (nextConfigNum == INITIAL_CONFIG_NUM) {
            return null;
        } else {
            return configsMap.get(nextConfigNum - 1);
        }
    }

    private Result processLeave(Leave l){
        int toLeave = l.groupId();
        if (latestConfig() == null || !latestConfig().groupInfo().containsKey(toLeave)) {
            return new Error();
        } else {
            withdrawGroup(latestConfig(), toLeave);
            return new Ok();
        }
    }

    private void withdrawGroup(ShardConfig latestConfig, int toLeave) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = latestConfig.groupInfo();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int newGroupNum = latestGroupInfo.size() - 1;
        int newAvarage = numShards / newGroupNum;
        int needsToFill = 0;
        for (Integer group : latestGroupInfo.keySet()) {
            if (group != toLeave) {
                Set<Integer> groupShards = new HashSet<>(latestGroupInfo.get(group).getRight());
                int shards = groupShards.size();
                needsToFill += Math.max(newAvarage - shards, 0);
                newGroupInfo.put(group, Pair.of(latestGroupInfo.get(group).getLeft(), groupShards));
            }
        }
        ArrayList<Integer> spareShards = new ArrayList<>(latestGroupInfo.get(toLeave).getRight());
        int fromRicher = needsToFill - spareShards.size();
        if (fromRicher > 0) {
            for (Integer group : newGroupInfo.keySet()) {
                if (group != toLeave) {
                    Set<Integer> groupShards = new HashSet<>(newGroupInfo.get(group).getRight());
                    if (groupShards.size() > newAvarage) {
                        int takeFrom = Math.min(groupShards.size() - newAvarage, fromRicher);
                        Iterator<Integer> it = groupShards.iterator();
                        Set<Integer> toRemove = new HashSet<>();
                        while (takeFrom > 0 && it.hasNext()) {
                            toRemove.add(it.next());
                            takeFrom--;
                            fromRicher--;
                        }
                        groupShards.removeAll(toRemove);
                        newGroupInfo.put(group, Pair.of(newGroupInfo.get(group).getLeft(), groupShards));
                        spareShards.addAll(toRemove);
                    }
                }
            }
        }
        int readHead = 0;
        for (Integer group : newGroupInfo.keySet()) {
            Set<Integer> groupShards = new HashSet<>(newGroupInfo.get(group).getRight());
            int count = newAvarage - groupShards.size();
            while (readHead < spareShards.size() && count > 0){
                groupShards.add(spareShards.get(readHead));
                readHead++;
                count--;
            }
            newGroupInfo.put(group, Pair.of(latestGroupInfo.get(group).getLeft(), groupShards));
        }
        if (readHead < spareShards.size()) {
            for (Integer group : newGroupInfo.keySet()) {
                Set<Integer> groupShards = newGroupInfo.get(group).getRight();
                if (readHead >= spareShards.size()) {
                    break;
                } else {
                    groupShards.add(spareShards.get(readHead));
                    readHead++;
                }
                newGroupInfo.put(group, Pair.of(latestGroupInfo.get(group).getLeft(), groupShards));
            }
        }
        ShardConfig newShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        configsMap.put(nextConfigNum, newShardConfig);
        nextConfigNum++;
    }

    private Result processMove(Move m){
        int toMove = m.shardNum();
        int targetGroup = m.groupId();
        if (this.nextConfigNum == INITIAL_CONFIG_NUM || !latestConfig().groupInfo().containsKey(targetGroup)) {
            return new Error();
        } else if(toMove < 1 || toMove > numShards) {
            return new Error();
        } else {
            Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = latestConfig().groupInfo();
            Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
            for (Integer group : latestGroupInfo.keySet()) {
                Pair<Set<Address>, Set<Integer>> groupDetail = latestGroupInfo.get(group);
                if (group == targetGroup && groupDetail.getRight().contains(toMove)) {
                    return new Error();
                } else if (group == targetGroup) {
                    groupDetail.getRight().add(toMove);
                } else if (groupDetail.getRight().contains(toMove)) {
                    groupDetail.getRight().remove(toMove);
                }
                newGroupInfo.put(group, groupDetail);
            }
            ShardConfig newShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
            configsMap.put(nextConfigNum, newShardConfig);
            nextConfigNum++;
            return new Ok();
        }
    }

    private Result processJoin(Join j){
        int groupId = j.groupId();
        Set<Address> servers = j.servers();
        if (nextConfigNum == INITIAL_CONFIG_NUM) {
            ShardConfig config = new ShardConfig(nextConfigNum, new HashMap<>());
            Set<Integer> shards = new HashSet<>();
            for (int i = 1;i <= this.numShards;i++){
                shards.add(i);
            }
            config.groupInfo.put(groupId,Pair.of(servers, shards));
            configsMap.put(nextConfigNum, config);
            nextConfigNum++;
            return new Ok();
        } else {
            ShardConfig latestConfig = latestConfig();
            assert(latestConfig != null);
            if (latestConfig.groupInfo.containsKey(groupId)) {
                return new Error();
            } else {
                joinGroup(latestConfig, groupId, servers);
                return new Ok();
            }
        }
    }

    private void joinGroup(ShardConfig latestConfig, int groupId, Set<Address> servers) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = latestConfig.groupInfo();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int newGroupNum = latestGroupInfo.size() + 1;
        int newAvarage = numShards / newGroupNum;
        int needsToFill = 0;
        for (Integer group : latestGroupInfo.keySet()) {
            Set<Integer> groupShards = new HashSet<>(latestGroupInfo.get(group).getRight());
            int shards = groupShards.size();
            needsToFill += Math.max(newAvarage - shards, 0);
            newGroupInfo.put(group, Pair.of(latestGroupInfo.get(group).getLeft(), groupShards));
        }
        needsToFill += newAvarage;
        newGroupInfo.put(groupId, Pair.of(servers, new HashSet<>()));
        ArrayList<Integer> spareShards = new ArrayList<>();
        if (needsToFill > 0) {
            for (Integer group : newGroupInfo.keySet()) {
                Set<Integer> groupShards = new HashSet<>(newGroupInfo.get(group).getRight());
                if (groupShards.size() > newAvarage) {
                    int takeFrom = Math.min(groupShards.size() - newAvarage, needsToFill);
                    Iterator<Integer> it = groupShards.iterator();
                    Set<Integer> toRemove = new HashSet<>();
                    while (takeFrom > 0 && it.hasNext()) {
                        toRemove.add(it.next());
                        takeFrom--;
                        needsToFill--;
                    }
                    groupShards.removeAll(toRemove);
                    newGroupInfo.put(group, Pair.of(newGroupInfo.get(group).getLeft(), groupShards));
                    spareShards.addAll(toRemove);
                }
            }
        }
        int readHead = 0;
        for (Integer group : newGroupInfo.keySet()) {
            Set<Integer> groupShards = new HashSet<>(newGroupInfo.get(group).getRight());
            int count = newAvarage - groupShards.size();
            while (readHead < spareShards.size() && count > 0){
                groupShards.add(spareShards.get(readHead));
                readHead++;
                count--;
            }
            newGroupInfo.put(group, Pair.of(newGroupInfo.get(group).getLeft(), groupShards));
        }
        if (readHead < spareShards.size()) {
            for (Integer group : newGroupInfo.keySet()) {
                Set<Integer> groupShards = newGroupInfo.get(group).getRight();
                if (readHead >= spareShards.size()) {
                    break;
                } else {
                    groupShards.add(spareShards.get(readHead));
                    readHead++;
                }
                newGroupInfo.put(group, Pair.of(newGroupInfo.get(group).getLeft(), groupShards));
            }
        }
        ShardConfig newShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        configsMap.put(nextConfigNum, newShardConfig);
        nextConfigNum++;
    }

    private Result processQuery(Query q){
        int queryConfigNum = q.configNum();
        if (this.nextConfigNum == INITIAL_CONFIG_NUM) {
            return new Error();
        } else if (queryConfigNum == -1 || queryConfigNum >= nextConfigNum) {
            return latestConfig();
        } else {
            return configsMap.get(queryConfigNum);
        }
    }
}
