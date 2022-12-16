import java.util.concurrent.locks.*;

public class Name implements Comparable<Name> {
    private String path;
    // private boolean readLock;
    // private boolean writeLock;
    protected final ReadWriteLock readWriteLock;

    // TODO: validate path, such as trim ...
    public Name(String path) {
        this.path = path;
        readWriteLock = new ReentrantReadWriteLock();
    }

    @Override
    public int compareTo(Name that) {
        return this.path.compareTo(that.getPath());
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        if (that.getClass() != this.getClass()) return false;
        if (this.path == null) return ((Name) that).getPath() == null;
        return this.path.equals(((Name) that).getPath());
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    public String getPath() {
        return path;
    }

    // public boolean getReadLock() {
    //     return readLock;
    // }

    // public boolean getWriteLock() {
    //     return writeLock;
    // }

    // public void lockRead() {
    //     readLock = true;
    // }

    // public void unlockRead() {
    //     readLock = false;
    // }

    // public void lockWrite() {
    //     writeLock = true;
    // }

    // public void unlockWrite() {
    //     writeLock = false;
    // }
}
