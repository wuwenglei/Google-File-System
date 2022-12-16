import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ChunkServerSender implements Callable<String> {

    private String ipAddress;
    private int port;
    private String message;

    public ChunkServerSender(String ipAddress, int port, String message) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.message = message;
    }

    public ChunkServerSender(NetAddress netAddress, String message) {
        this(netAddress.getIpAddress(), netAddress.getPort(), message);
    }

    @Override
    public String call() {
        Socket socket = null;
        OutputStream os = null;
        InputStream is = null;
        ByteArrayOutputStream baos = null;
        String reply = "444 Reply did not received!";
        try {
            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            socket = new Socket(inetAddress, port);
            os = socket.getOutputStream();
            os.write(message.getBytes("UTF-8"));
            socket.shutdownOutput();
            is = socket.getInputStream();
            baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = is.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            reply = baos.toString();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return reply;
    }
}
