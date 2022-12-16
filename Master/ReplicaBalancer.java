import java.util.*;
import java.util.concurrent.*;

public class ReplicaBalancer implements Runnable {

    protected static final int replicaPerChunk = 2;
    private static ConcurrentMap<Integer, Integer> chunkServerMappingReplicaCounts = new ConcurrentHashMap<Integer, Integer>();
    private static ConcurrentMap<Integer, Set<Integer>> replicaCountMappingChunkServers = new ConcurrentSkipListMap<Integer, Set<Integer>>();
    private static int sumReplica = 0; // Chunk handle is long, maybe this should also be long.

    public static List<Integer> assignReplicaLocationsToChunk(Chunk chunk) {
        List<Integer> chunkservers = new ArrayList<Integer>(replicaPerChunk);
        for (Set<Integer> set: replicaCountMappingChunkServers.values()) {
            for (Integer chunkserverID: set) {
                chunkservers.add(chunkserverID);
                if (chunkservers.size() == replicaPerChunk) {
                    break;
                }
            }
            if (chunkservers.size() == replicaPerChunk) break;
        }
        chunk.addAllReplicaLocations(chunkservers);
        chunkservers.forEach((e) -> {
            addReplicaToChunkServer(e, 1);
        });
        if (chunkservers.size() < replicaPerChunk)
            System.err.println(String.format("The number of chunkservers (%d) does not support the number of replica per chunk (%d), fewer replicas will be generated.", chunkservers.size(), replicaPerChunk));
        return chunkservers;
    }

    public static void addReplicaToChunkServer(int chunkserverID, int countReplica) {
        chunkServerMappingReplicaCounts.putIfAbsent(chunkserverID, 0);
        int previousCount = chunkServerMappingReplicaCounts.put(chunkserverID, chunkServerMappingReplicaCounts.get(chunkserverID) + countReplica);
        int newCount = previousCount + countReplica;
        addSumReplica(newCount - previousCount);
        if (replicaCountMappingChunkServers.containsKey(previousCount)) {
            replicaCountMappingChunkServers.get(previousCount).remove(chunkserverID);
            if (replicaCountMappingChunkServers.get(previousCount).size() == 0)
                replicaCountMappingChunkServers.remove(previousCount);
        }
        replicaCountMappingChunkServers.putIfAbsent(newCount, Collections.synchronizedSet(new HashSet<Integer>()));
        replicaCountMappingChunkServers.get(newCount).add(chunkserverID);
    }

    public static void removeReplicaFromChunkServer(int chunkserverID, int countReplica) {
        if (chunkServerMappingReplicaCounts.containsKey(chunkserverID)) {
            int previousCount = chunkServerMappingReplicaCounts.put(chunkserverID, Math.max(0, chunkServerMappingReplicaCounts.get(chunkserverID) - countReplica));
            int newCount = Math.max(0, previousCount - countReplica);
            addSumReplica(newCount - previousCount);
            if (replicaCountMappingChunkServers.containsKey(previousCount)) {
                replicaCountMappingChunkServers.get(previousCount).remove(chunkserverID);
                if (replicaCountMappingChunkServers.get(previousCount).size() == 0)
                    replicaCountMappingChunkServers.remove(previousCount);
            }
            replicaCountMappingChunkServers.putIfAbsent(newCount, Collections.synchronizedSet(new HashSet<Integer>()));
            replicaCountMappingChunkServers.get(newCount).add(chunkserverID);
        }
        System.err.println("Nothing happened: chunkServerMappingReplicaCounts does not contains key " + String.valueOf(chunkserverID));
    }

    private static synchronized void addSumReplica(int countReplica) {
        sumReplica += countReplica;
    }

    public void run() {

    }

}
