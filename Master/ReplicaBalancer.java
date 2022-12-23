import java.util.*;
import java.util.concurrent.*;
// Goal: equalizing the number of replicas on each chunkserver. Disk space utilization is not considered, though different chunkservers may have different disk space.
public class ReplicaBalancer implements Runnable {

    protected static final int replicaPerChunk = 2;
    private static ConcurrentMap<Integer, Integer> chunkServerMappingReplicaCounts = new ConcurrentHashMap<Integer, Integer>();
    private static ConcurrentMap<Integer, Set<Integer>> replicaCountMappingChunkServers = new ConcurrentSkipListMap<Integer, Set<Integer>>();
    private static int sumReplica = 0; // Chunk handle is long, maybe this should also be long. // not sure if needed, consider this when implementing run(), if needed then call addSUmRelica() in other methods

    public static List<Integer> assignReplicaLocationsToChunk(Chunk chunk) {
        int nReplica = Math.max(0, replicaPerChunk - chunk.getReplicaLocations().size());
        List<Integer> chunkservers = new ArrayList<Integer>(nReplica);
        for (Set<Integer> set: replicaCountMappingChunkServers.values()) {
            if (chunkservers.size() == nReplica) break;
            for (Integer chunkserverID: set) {
                if (chunkservers.size() == nReplica) break;
                chunkservers.add(chunkserverID);
            }
        }
        chunk.addAllReplicaLocations(chunkservers);
        chunkservers.forEach((e) -> {
            addReplicaToChunkServer(e, 1);
        });
        if (chunkservers.size() < nReplica)
            System.err.println(String.format("The number of chunkservers (%d) did not support the number of replicas to be generated (%d), fewer replicas was generated.", chunkservers.size(), replicaPerChunk));
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

    public void run() { // See paper 4.3
        // Check connectivity of chunkservers, chunkservers reply with replicas they store: see Master.readLogChunkServer(), Master.queryChunkLocation(); remove replicas in disconnected chunkservers

        // Assign chunkserver to chunks having less than replicaPerChunk replicas, and assign replica to chunkservers

        // Remove chunkserver from chunks having more than replicaPerChunk replicas, and remove replica from chunkservers

        // Move replica from chunkservers having above average number of replicas to chunkservers having below average number of replicas
    }

}
