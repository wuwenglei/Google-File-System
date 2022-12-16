import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.rmi.UnexpectedException;

public class ChunkServerReceiver implements Runnable {

    private int port;

    public ChunkServerReceiver(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            ExecutorService pool = Executors.newCachedThreadPool();
            while (true) {
                try {
                    pool.execute(new ChunkServerReceiverHandler(serverSocket.accept()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ChunkServerReceiverHandler implements Runnable {

        private Socket socket;

        public ChunkServerReceiverHandler(Socket socket) {
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
                    reply = register(message);
                } else if (commandID == 1) {
                    reply = unregister(message);
                } else if (commandID == 2) {
                    reply = connect(message, socket);
                } else if (commandID == 3) {
                    reply = disconnect(message);
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

    private String register(String[] message) { // Register request
        String reply;
        if (message.length == 1) {
            try {
                reply = "666 " + String.valueOf(Master.registerChunkserver());
            } catch (IndexOutOfBoundsException | UnexpectedException e) {
                e.printStackTrace();
                reply = "444 Registration failed! " + e.getMessage();
            }
        } else {
            reply = "444 Register request (0) format is invalid!";
        }
        return reply;
    }

    private String unregister(String[] message) { // Unregister request: commandID chunkserverID ///////////// Check with other functionalities // Check registeredChunkserver thread-safety
        String reply;
        if (message.length != 2) {
            reply = "444 Unregister request (1) format is invalid!";
        } else if (! Master.registeredChunkserver.contains(Integer.valueOf(message[1]))) {
            reply = "444 Chunkserver is not registered!";
        } else {
            Master.unregisterChunkserver(Integer.valueOf(message[1]));
            Master.connectedChunkServers.remove(Integer.valueOf(message[1]));
            reply = "666 Successfully unregistered!";
        }
        return reply;
    }

    private String connect(String[] message, Socket socket) { // Connect request: commandID chunkserverID
        String reply;
        if (message.length != 2) {
            reply = "444 Connect request (2) format is invalid!";
        } else if (! Master.registeredChunkserver.contains(Integer.valueOf(message[1]))) {
            reply = "444 Chunkserver is not registered!";
        } else {
            Master.connectedChunkServers.put(Integer.valueOf(message[1]), new NetAddress(socket.getRemoteSocketAddress()));
            reply = "666 Connection established!";
        }
        return reply;
    }

    private String disconnect(String[] message) { // Disconnect request: commandID chunkserverID 
        String reply;
        if (message.length != 2) {
            reply = "444 Disconnect request (3) format is invalid!";
        } else if (! Master.connectedChunkServers.containsKey(Integer.valueOf(message[1]))) {
            reply = "444 Chunkserver is not registered!";
        } else {
            Master.connectedChunkServers.remove(Integer.valueOf(message[1]));
            // TODO: check this, what if chunkserver dead, what shall I do?

            reply = "666 Successfully unregistered!";
        }
        return reply;
    }
}