import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.rmi.UnexpectedException;

public class ClientReceiver implements Runnable {

    private int port;

    public ClientReceiver(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            ExecutorService pool = Executors.newCachedThreadPool();
            while (true) {
                try {
                    pool.execute(new ClientReceiverHandler(serverSocket.accept()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ClientReceiverHandler implements Runnable {

        private Socket socket;

        public ClientReceiverHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (InputStream is = socket.getInputStream();
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    OutputStream os = socket.getOutputStream()) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = is.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, length);
                }
                String[] message = byteArrayOutputStream.toString("UTF-8").trim().split(" ");
                int commandID = Integer.parseInt(message[0]);
                String reply = "444 Invalid CommandID!";
                if (commandID == 0) {
                    reply = create(message);
                } else if (commandID == 1) {
                    reply = delete(message);
                } else if (commandID == 2) {
                    reply = read(message);
                } else if (commandID == 3) {
                    reply = write(message);
                } else if (commandID == 100) {
                    reply = releaseLock(message);
                }
                os.write(reply.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String create(String[] message) { // Create request: commandID, namespace, size
        String reply;
        // TODO: Check unlock: finished transmission; Reply message: need size or handle or both?; load balancing
        if (message.length < 3) {
            reply = "444 Create request (0) format is invalid!";
        } else if (Master.namespace.containsKey(message[1])) {
            reply = "444 File name exists: " + message[1];
        } else {
            List<Name> names = Master.addToNameSpace(message[1]);
            Name name = names.get(names.size() - 1);
            name.readWriteLock.writeLock().lock(); // TODO: Or tryLock?
            for (int i = names.size() - 2; i >= 0; i--) {
                names.get(i).readWriteLock.readLock().lock();
            }
            List<Chunk> chunks = Master.mapping.getOrDefault(name, new ArrayList<Chunk>());
            Master.mapping.put(name, chunks);
            StringBuilder sb = new StringBuilder("666");
            long sizeRemaining = Long.parseLong(message[2]);
            while (sizeRemaining > 0) {
                int usedSize = sizeRemaining > Chunk.SIZE ? Chunk.SIZE : (int) sizeRemaining;
                Chunk chunk = new Chunk(Master.incrementChunkHandle());
                chunks.add(chunk);
                Master.chunkInfo.put(chunk.getHandle(), chunk);
                sizeRemaining -= usedSize;
                sb.append(' ');
                sb.append(chunk.getHandle());
            }
            reply = sb.toString(); // status handle handle handle ...
        }
        return reply;
    }

    private String delete(String[] message) { // Delete request
        String reply;

        return reply;
    }

    private String read(String[] message) { // Read request
        String reply;

        return reply;
    }

    private String write(String[] message) { // Write request
        String reply;

        return reply;
    }

    private String releaseLock(String[] message) { // Release lock: commandID operationCommandID path
        String reply;

        return reply;
    }
}
