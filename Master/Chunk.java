import java.util.*;

public class Chunk {
    protected static final int SIZE = 64 * 1024 * 1024;
    private final long HANDLE;
    private int version;
    private int occupiedSize;
    private Map<Name, Integer> fileSizes;
    private Set<Integer> replicaLocations;
    private int totalFileSize;
    private Integer lease;

    public Chunk(long handle) {
        HANDLE = handle;
        version = 0;
        fileSizes = new HashMap<Name, Integer>();
        replicaLocations = Collections.synchronizedSet(new HashSet<Integer>());
        totalFileSize = 0;
        lease = null;
    }

    public Chunk(long handle, int version) {
        this(handle);
        this.version = version;
    }

    public int getReplicaCount() {
        return replicaLocations.size();
    }

    public long getHandle() {
        return HANDLE;
    }

    public int getVersion() {
        return version;
    }

    public int getOccupiedSize() {
        return occupiedSize;
    }

    public boolean addReplicaLocation(int chunkserverID) {
        return replicaLocations.add(chunkserverID);
    }

    public boolean removeReplicaLocation(int chunkserverID) {
        return replicaLocations.remove(chunkserverID);
    }

    public boolean addAllReplicaLocations(Collection<? extends Integer> c) {
        return replicaLocations.addAll(c);
    }

    public int getTotalFileSize() {
        return totalFileSize;
    }

    public int getRemainingSize() {
        return Chunk.SIZE - totalFileSize;
    }

    // @Override
    // public String toString() {

    // }

    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        if (that.getClass() != this.getClass()) return false;
        return this.HANDLE == ((Chunk) that).HANDLE;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(HANDLE).hashCode();
    }

    public Integer getLease() {
        return lease;
    }

    public void setLease(Integer lease) {
        this.lease = lease;
    }
}
