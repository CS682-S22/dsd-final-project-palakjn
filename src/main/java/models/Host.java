package models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for holding host information
 *
 * @author Palak Jain
 */
public class Host {
    @Expose
    private int id;
    @Expose
    private String address;
    @Expose
    private int port;

    public Host(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    /**
     * Get the identifier of the host
     */
    public int getId() {
        return id;
    }

    /**
     * Set the identifier of the host
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Get the address of the host
     */
    public String getAddress() {
        return address;
    }

    /**
     * Set the address of the host
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Get the port of the host
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port of the host
     */
    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", address, port);
    }
}
