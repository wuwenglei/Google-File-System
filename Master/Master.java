import java.io.*;
import java.util.logging.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;
import java.nio.file.*;
import java.rmi.UnexpectedException;

public class Master {
    
    private static long chunkHandleGlobalIncrementer = 0L; // TODO: log this
    private static int chunkserverLimit = 1000;

    private static final Logger NAMESPACE_LOGGER = Logger.getLogger("namespace");
    private static final Logger MAPPING_LOGGER = Logger.getLogger("mapping");
    private static final Logger CHUNKSERVER_LOGGER = Logger.getLogger("chunkserver");
    protected static final String namespaceLogFileName = "./namespace.log";
    protected static final String mappingLogFileName = "./mapping.log";
    protected static final String chunkserverLogFileName = "./chunkserver.log";

    protected static final ReplicaBalancer REPLICA_BALANCER = new ReplicaBalancer();

    protected static ConcurrentMap<String, Name> namespace = new ConcurrentHashMap<String, Name>();
    protected static ConcurrentMap<Name, List<Chunk>> mapping = new ConcurrentHashMap<Name, List<Chunk>>();
    protected static Set<Integer> registeredChunkserver = Collections.synchronizedSet(new HashSet<Integer>());
    protected static ConcurrentMap<Integer, NetAddress> connectedChunkServers = new ConcurrentHashMap<Integer, NetAddress>();
    private static Set<Chunk> incompleteChunks = new TreeSet<Chunk>((a, b) -> Integer.compare(a.getRemainingSize(), b.getRemainingSize()));
    protected static Map<Long, Chunk> chunkInfo = new ConcurrentHashMap<Long, Chunk>();

    // TODO: operation logs; metadata is logged instead.

    public static synchronized int registerChunkserver() throws UnexpectedException {
        for (int i = 0; i < chunkserverLimit; i++) {
            if (! registeredChunkserver.contains(i)) {
                registeredChunkserver.add(i);
                writeLogChunkServer(i, CHUNKSERVER_LOGGER);
                return i;
            }
        }
        throw new IndexOutOfBoundsException("Exceed chunkserver limit: " + String.valueOf(chunkserverLimit));
    }

    public static synchronized boolean unregisterChunkserver(int chunkserverID) {
        return registeredChunkserver.remove(chunkserverID);
    }

    public static List<Name> addToNameSpace(String path) {
        if (path.matches(".*/{2,}.*")) {
            throw new IllegalArgumentException("Path should not contain more than one consecutive '/'.");
        }
        List<Name> res = new ArrayList<Name>();
        String[] names = path.trim().split("/+");
        StringBuilder sb = new StringBuilder();
        for (String name: names) {
            name = name.trim();
            if (name.length() == 0) continue;
            sb.append('/');
            sb.append(name);
            String p = sb.toString();
            Name tmp = new Name(p);
            if (! namespace.containsKey(p)) {
                namespace.put(p, tmp);
                writeLogNamespace(tmp, NAMESPACE_LOGGER);
                mapping.put(tmp, new ArrayList<Chunk>());
            }
            res.add(namespace.get(p));
        }
        return res;
    }

    public static List<Name> getNameInstancesOfPath(String path) {
        if (path.matches(".*/{2,}.*")) {
            throw new IllegalArgumentException("Path should not contain more than one consecutive '/'.");
        }
        List<Name> res = new ArrayList<Name>();
        String[] names = path.trim().split("/+");
        StringBuilder sb = new StringBuilder();
        for (String name: names) {
            name = name.trim();
            if (name.length() == 0) continue;
            sb.append('/');
            sb.append(name);
            String p = sb.toString();
            if (! namespace.containsKey(p)) {
                throw new IllegalArgumentException("Invalid path: " + path);
            }
            res.add(namespace.get(p));
        }
        return res;
    }

    public static synchronized long incrementChunkHandle() {
        return chunkHandleGlobalIncrementer++;
    }

    public static void display(String s) {
        System.out.println(s);
    }

    public static void initializeLogger(Logger logger, String filename, boolean append) {
        Handler fileHandler  = null;
        try{
            fileHandler = new FileHandler(filename, append);
            logger.addHandler(fileHandler);
            fileHandler.setLevel(Level.ALL);
            fileHandler.setFormatter(new SimpleFormatter(){
                @Override
                public String format(LogRecord record) {
                    return record.getMessage()+"\n";
                }
            });
            logger.setLevel(Level.ALL);
            // LOGGER.config("Configuration done.");
        }catch(IOException | SecurityException e){
            e.printStackTrace();
        }
    }

    public static void writeLog(Logger logger, String s) {
        logger.log(Level.INFO, s);
    }

    public static void writeLogNamespace(Name name, Logger logger) {
        writeLog(logger, name.getPath());
    }

    public static void writeLogMapping(Map.Entry<Name, List<Chunk>> entry, Logger logger) {
        StringBuilder sb = new StringBuilder();
        sb.append(entry.getKey().getPath());
        sb.append(' ');
        for (Chunk chunk: entry.getValue()) {
            sb.append(chunk.getHandle());
            sb.append(' ');
        }
        writeLog(logger, sb.toString().trim());
    }

    public static void writeLogChunkServer(int chunkserverID, Logger logger) throws UnexpectedException {
        StringBuilder sb = new StringBuilder(String.valueOf(chunkserverID));
        if (connectedChunkServers.containsKey(chunkserverID)) {
            sb.append(" 1 ");
            sb.append(connectedChunkServers.get(chunkserverID).toString());
        } else if (registeredChunkserver.contains(chunkserverID)) {
            sb.append(" 0");
        } else {
            throw new UnexpectedException("Chunkserver is not registered: " + String.valueOf(chunkserverID));
        }
        writeLog(logger, sb.toString().trim());
    }

    public static void readLogNamespace(String filename) {
		try (FileReader fr = new FileReader(filename);
                BufferedReader br = new BufferedReader(fr)) {
			String line = br.readLine().trim();
			while (line != null) {
				if (line.length() > 0) {
                    Name name = new Name(line);
                    namespace.put(line, name); // What if duplicate found in log file?
                    mapping.put(name, new ArrayList<Chunk>());
                }
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    // TODO: file damaged
    public static void readLogMapping(String filename) { // namespace handle handle handle ...
        try (FileReader fr = new FileReader(filename);
                BufferedReader br = new BufferedReader(fr)) {
			String line = br.readLine().trim();
			while (line != null) {
				if (line.length() > 0) {
                    String[] formatted = line.split(" ");
                    Name name = namespace.get(formatted[0]);
                    List<Chunk> chunksForName = mapping.getOrDefault(name, new ArrayList<Chunk>());
                    for (int i = 1; i < formatted.length; i++) {
                        Chunk chk = chunkInfo.get(Long.valueOf(formatted[i]));
                        if (chk != null) {
                            chunksForName.add(chk);
                        } else { // a chunk does not exist in any of the connected chunkservers, or a connected chunkserver did not reply with its chunk info
                            // file damaged
                        }
                    }
                    mapping.put(name, chunksForName);
                }
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    // public static void readLogChunkServer(String filename) { // ChunkserverID status ipAddress port
    //     ExecutorService pool = Executors.newCachedThreadPool();
	// 	try (FileReader fr = new FileReader(filename);
    //             BufferedReader br = new BufferedReader(fr)) {////////////////////////////////modify
	// 		String line = br.readLine().trim();
	// 		while (line != null) {
	// 			if (line.length() > 0) {
    //                 String[] formatted = line.split(" ");
    //                 if (Integer.parseInt(formatted[1]) == 1) { // Was connected
    //                     try {
    //                         if (pool.submit(new ChunkServerSender(formatted[2], Integer.parseInt(formatted[3]), "0")).get(10, TimeUnit.SECONDS).trim().equals("666")) { // Is still connectable    0: Connection verification message
    //                             connectedChunkServers.put(Integer.parseInt(formatted[0]), new NetAddress(formatted[2], formatted[3]));
    //                         }
    //                     } catch (ExecutionException | InterruptedException e) {
    //                         e.printStackTrace();
    //                     } catch (TimeoutException e) {
    //                         e.printStackTrace();
    //                         System.err.println("Timeout verifying connection of chunkserver " + formatted[0] + ", no longer connected.");
    //                     }
    //                 }
    //                 registeredChunkserver.add(Integer.parseInt(formatted[0]));
    //             }
	// 			line = br.readLine();
	// 		}
	// 	} catch (IOException e) {
	// 		e.printStackTrace();
	// 	}
    //     pool.shutdown();
    // }
    public static void readLogChunkServer(String filename) { // ChunkserverID status ipAddress port
        List<String[]> formatteds = new ArrayList<String[]>();
        List<ChunkServerSender> chunkServerSenders = new ArrayList<ChunkServerSender>();
		try (FileReader fr = new FileReader(filename);
                BufferedReader br = new BufferedReader(fr)) {
			String line = br.readLine().trim();
			while (line != null) {
				if (line.length() > 0) {
                    String[] formatted = line.split(" ");
                    if (Integer.parseInt(formatted[1]) == 1) { // Was connected
                        formatteds.add(formatted);
                        chunkServerSenders.add(new ChunkServerSender(formatted[2], Integer.parseInt(formatted[3]), "0")); // 0: Connection verification message
                    }
                    registeredChunkserver.add(Integer.parseInt(formatted[0]));
                }
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<String>> results = pool.invokeAll(chunkServerSenders, 10, TimeUnit.SECONDS);
            for (int i = 0; i < results.size(); i++) {
                String[] formatted = formatteds.get(i);
                try {
                    Future<String> result = results.get(i);
                    if (result.isCancelled()) {
                        System.err.println("Timeout verifying connection of chunkserver " + formatted[0] + ", no longer connected.");
                    } else {
                        if (result.get().trim().equals("666")) { // Is still connectable
                            connectedChunkServers.put(Integer.parseInt(formatted[0]), new NetAddress(formatted[2], formatted[3]));
                        }
                    }
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                } catch (CancellationException e) {
                    e.printStackTrace();
                    System.err.println("Timeout verifying connection of chunkserver " + formatted[0] + ", no longer connected.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        pool.shutdown();
    }

    // TODO: versions in all chunks should be updated in this method; handle UnexpectedException
    public static void queryChunkLocation() {
        List<Integer> failedChunkservers = new ArrayList<Integer>(); // Can be removed: optimize code
        List<Integer> chunkserverIDs = new ArrayList<Integer>();
        List<ChunkServerSender> chunkServerSenders = new ArrayList<ChunkServerSender>();
        connectedChunkServers.forEach((key, value) -> {
            chunkserverIDs.add(key);
            chunkServerSenders.add(new ChunkServerSender(value.getIpAddress(), value.getPort(), "1")); // 1: Get chunk information message
        });
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<String>> results = pool.invokeAll(chunkServerSenders, 10, TimeUnit.SECONDS);
            for (int i = 0; i < results.size(); i++) {
                try {
                    Future<String> result = results.get(i);
                    if (result.isCancelled()) {
                        System.err.println("Timeout getting reply from chunkserver " + chunkserverIDs.get(i).toString() + ", no longer connected.");
                        failedChunkservers.add(chunkserverIDs.get(i));
                    } else {
                        String[] chunks = result.get().trim().split(" "); // status handle version handle version ... 
                        if (chunks[0] == "444") {
                            throw new UnexpectedException("Chunkserver " + chunkserverIDs.get(i).toString() + " failed to retrieve chunk information.");
                        } else {
                            ReplicaBalancer.addReplicaToChunkServer(chunkserverIDs.get(i), (chunks.length - 1) / 2); // TODO: coordinate with version below
                            for (int j = 1; j < chunks.length - 1; j += 2) {
                                long handle = Long.valueOf(chunks[j]);
                                int version = Integer.valueOf(chunks[j + 1]);
                                Chunk chk = chunkInfo.getOrDefault(handle, new Chunk(handle, version));
                                chk.addReplicaLocation(chunkserverIDs.get(i)); // Do I need to check boolean return value?
                                if (version < chk.getVersion()) {
                                    // Send update to low version chunkserver, if failed remove from replica locations
                                } else if (version > chk.getVersion()) {
                                    // Update chk, and send send update to Send update to low version chunkserver, if failed remove from replica locations
                                }
                                chunkInfo.put(handle, chk);
                            }
                        }
                    }
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                } catch (CancellationException e) {
                    e.printStackTrace();
                    System.err.println("Timeout getting reply from chunkserver " + chunkserverIDs.get(i).toString() + ", no longer connected.");
                    failedChunkservers.add(chunkserverIDs.get(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        failedChunkservers.forEach((a) -> connectedChunkServers.remove(a));
        pool.shutdown();
    }

    public static void refreshLogs() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
        for (int i = 0; i < 2; i++) {
            executor.scheduleWithFixedDelay(new ScheduledLogUpdater(i), 30, 30, TimeUnit.SECONDS);
        }
    }

    public static void listenToChunkServer() {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 11000; i < 11010; i++) {
            pool.execute(new ChunkServerReceiver(i));
        }
    }

    public static void listenToClient() {
        ExecutorService pool = Executors.newFixedThreadPool(50);
        for (int i = 12000; i < 12050; i++) {
            pool.execute(new ClientReceiver(i));
        }
    }

    public static void refreshReplicaDistribution() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleWithFixedDelay(REPLICA_BALANCER, 300, 300, TimeUnit.SECONDS);
    }

    public static void masterStart() {
        display("System start.");
        display("Initializing...");
        initializeLogger(NAMESPACE_LOGGER, namespaceLogFileName, true);
        initializeLogger(MAPPING_LOGGER, mappingLogFileName, true);
        initializeLogger(CHUNKSERVER_LOGGER, chunkserverLogFileName, true);
        display("Retrieving operation logs...");
        display("Loading connected chunkserver");
        readLogChunkServer(chunkserverLogFileName);
        display("Loading chunk locations from connected chunkservers...");
        queryChunkLocation();
        display("Loading namespace...");
        readLogNamespace(namespaceLogFileName);
        display("Loading mappings...");
        readLogMapping(mappingLogFileName);
        display("Operation logs retrieved.");
        display("Begin to listen to chunkservers...");
        listenToChunkServer();
        display("Begin to listen to clients...");
        listenToClient();
        display("Initialization finished. Master ready!");
    }

    public static void main(String[] args) {
        masterStart();
        refreshLogs(); // check
        refreshReplicaDistribution();
    }
}