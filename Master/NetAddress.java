import java.net.Socket;
import java.net.SocketAddress;

import java.net.*;
public class NetAddress {
    private String ipAddress;
    private int port;

    public NetAddress(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public NetAddress(String ipAddress, String port) {
        this(ipAddress, Integer.parseInt(port));
    }

    public NetAddress(SocketAddress sa) {
        this.ipAddress = ((InetSocketAddress) sa).getAddress().toString().substring(1);
        this.port = ((InetSocketAddress) sa).getPort();
    }

    public NetAddress(Socket socket) {
        this(socket.getRemoteSocketAddress());
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return ipAddress + " " + String.valueOf(port);
    }
}
