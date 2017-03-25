package naming;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import rmi.*;
import common.*;
import storage.*;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    private Skeleton<Registration> registrationSkeleton;
    private Skeleton<Service> serviceSkeleton;
    private FSNode root;
    private List<Storage> storages;
    private List<Command> commands;

    static class FSNode {

        List<Integer> servers;
        Map<String, FSNode> children;
        StampedLock stampedLock = new StampedLock();
        int readCount = 0;

        FSNode(boolean isDirectory) {
            if (isDirectory) {
                children = new HashMap<>();
            } else {
                servers = new ArrayList<>();
            }
        }

        boolean isDirectory() {
            return children != null;
        }

        FSNode find(Path path) throws FileNotFoundException {
            FSNode curr = this;
            for (String name : path) {
                curr = curr.find(name);
            }
            return curr;
        }

        FSNode find(Path path, boolean isDirectory) throws FileNotFoundException {
            FSNode f = find(path);
            if (f.isDirectory() != isDirectory) {
                throw new FileNotFoundException();
            }
            return f;
        }

        FSNode find(String name) throws FileNotFoundException {
            if (!isDirectory()) {
                throw new FileNotFoundException();
            }
            FSNode node = children.get(name);
            if (node == null) {
                throw new FileNotFoundException();
            }
            return node;
        }

        void eachFile(Consumer<FSNode> consumer) {
            if (isDirectory()) {
                for (FSNode node : children.values()) {
                    node.eachFile(consumer);
                }
            } else {
                consumer.accept(this);
            }
        }

        void lock(boolean exclusive) {
            if (exclusive) {
                stampedLock.writeLock();
            } else {
                stampedLock.readLock();
            }
        }

        void unlock(boolean exclusive) {
            if (exclusive) {
                stampedLock.tryUnlockWrite();
            } else {
                stampedLock.tryUnlockRead();
            }
        }
    }

    public NamingServer()
    {
        InetSocketAddress registrationAddr = new InetSocketAddress(NamingStubs.REGISTRATION_PORT);
        InetSocketAddress serviceAddr = new InetSocketAddress(NamingStubs.SERVICE_PORT);

        registrationSkeleton = new Skeleton<Registration>(Registration.class, this, registrationAddr);
        serviceSkeleton = new Skeleton<Service>(Service.class, this, serviceAddr);
        root = new FSNode(true);
        storages = new ArrayList<>();
        commands = new ArrayList<>();
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        registrationSkeleton.start();
        serviceSkeleton.start();
    }

    /** Stops the naming server.

        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
        registrationSkeleton.stop();
        serviceSkeleton.stop();
        stopped(null);
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws RMIException, FileNotFoundException
    {
        FSNode node = root.find(path);
        if (!path.isRoot()) {
            lock(path.parent(), false);
        }

        node.lock(exclusive);

        if (!node.isDirectory()) {
            if (exclusive) {
                int index = node.servers.get(0);
                for (int server : node.servers) {
                    if (server != index) {
                        if (!commands.get(server).delete(path)) {
                            throw new IllegalStateException();
                        };
                    }
                }
                node.servers = Arrays.asList(index);
                node.readCount = 0;
            } else {
                node.readCount += 1;

                if (node.readCount == 20) {
                    int index = node.servers.size();
                    if (index <= storages.size()) {
                        Storage srcServer = storages.get(0);

                        try {
                            commands.get(index).copy(path, srcServer);
                            node.servers.add(index);
                        } catch (IOException e) {
                            throw new RMIException(e);
                        }
                    }
                    node.readCount = 0;
                }
            }
        }
    }

    @Override
    public void unlock(Path path, boolean exclusive) throws RMIException
    {
        try {
            FSNode node = root.find(path);
            if (!path.isRoot()) {
                unlock(path.parent(), false);
            }

            node.unlock(exclusive);
        } catch (FileNotFoundException | IllegalMonitorStateException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        return root.find(path).isDirectory();
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
        FSNode dir = root.find(directory, true);
        return dir.children.keySet().toArray(new String[dir.children.size()]);
    }

    @Override
    public boolean createFile(Path file) throws RMIException, FileNotFoundException
    {
        if (file.isRoot()) {
            return false;
        }

        FSNode dir = root.find(file.parent(), true);
        if (dir.children.containsKey(file.last())) {
            return false;
        }

        int serverIndex = 0;
        if (serverIndex >= storages.size()) {
            throw new IllegalStateException();
        }
        commands.get(serverIndex).create(file);

        FSNode node = new FSNode(false);
        node.servers.add(serverIndex);
        dir.children.put(file.last(), node);
        return true;
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
        if (directory.isRoot()) {
            return false;
        }

        FSNode dir = root.find(directory.parent(), true);
        if (dir.children.containsKey(directory.last())) {
            return false;
        }

        FSNode d = new FSNode(true);
        dir.children.put(directory.last(), d);
        return true;
    }

    @Override
    public boolean delete(Path path) throws RMIException, FileNotFoundException
    {
        if (path.isRoot()) {
            return false;
        }

        FSNode dir = root.find(path.parent(), true);
        FSNode node = dir.find(path.last());

        Set<Integer> servers = new HashSet<>();
        node.eachFile(n -> servers.addAll(n.servers));

        boolean success = true;
        for (int server : servers) {
            success = success && commands.get(server).delete(path);
        }
        dir.children.remove(path.last());
        return success;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        FSNode f = root.find(file, false);
        return storages.get(f.servers.get(0));
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        if (client_stub == null || command_stub == null || files == null) {
            throw new NullPointerException();
        }
        if (storages.contains(client_stub) || commands.contains(command_stub)) {
            throw new IllegalStateException();
        }

        root.lock(true);

        try {
            int serverIndex = storages.size();
            storages.add(client_stub);
            commands.add(command_stub);

            ArrayList<Path> ret = new ArrayList<>();

            for (Path path : files) {
                if (path.isRoot()) {
                    continue;
                }

                FSNode curr = root;
                for (String name : path.parent()) {
                    if (!curr.children.containsKey(name)) {
                        curr.children.put(name, new FSNode(true));
                    }
                    curr = curr.children.get(name);
                }

                String name = path.last();
                if (curr.children.containsKey(name)) {
                    ret.add(path);
                } else {
                    FSNode file = new FSNode(false);
                    file.servers.add(serverIndex);
                    curr.children.put(name, file);
                }
            }

            return ret.toArray(new Path[ret.size()]);
        } finally {
            root.unlock(true);
        }
    }
}
