package server.controllers;

import models.Host;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains the details of all the nodes which are part of the zone.
 *
 * @author Palak Jain
 */
public class Members {
    private Host local;
    private Map<Integer, Host> members;

    public Members() {
        members = new HashMap<>();
    }

    /**
     * Set the details of the server which is running the current instance
     */
    public void setLocal(Host local) {
        this.local = local;
    }

    /**
     * Get the details of the server which is running the current instance
     */
    public Host getLocalHost() {
        return local;
    }

    /**
     * Add the member information to the membership table if not exist before
     */
    public void addMember(Host member) {
        if (!isExist(member)) {
            members.put(member.getId(), member);
        }
    }

    /**
     * Get the member from the membership table with the given id
     */
    public Host getMember(int id) {
        return members.getOrDefault(id, null);
    }

    /**
     * Checks whether the given host exist in the membership table or not
     */
    public boolean isExist(Host host) {
        boolean isExist = false;
        Host member = members.getOrDefault(host.getId(), null);

        if (member != null) {
            isExist = member.equals(host);
        }

        return isExist;
    }

    /**
     * Get the list of other members in the zone
     */
    public List<Host> getNeighbors() {
        return members.values().stream().toList();
    }
}
