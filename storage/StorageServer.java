package storage;

import java.io.*;
import java.net.*;

import common.*;
import rmi.*;
import naming.*;

/**
 * Storage server.
 * <p>
 * <p>
 * Storage servers respond to client file access requests. The files accessible
 * through a storage server are those accessible under a given directory of the
 * local filesystem.
 */
public class StorageServer implements Storage, Command {

    private File root;
    private Skeleton<Storage> storageSkeleton;
    private Skeleton<Command> commandSkeleton;
    private boolean started;

    /**
     * Creates a storage server, given a directory on the local filesystem, and
     * ports to use for the client and command interfaces.
     * <p>
     * <p>
     * The ports may have to be specified if the storage server is running
     * behind a firewall, and specific ports are open.
     *
     * @param root
     * Directory on the local filesystem. The contents of this
     * directory will be accessible through the storage server.
     * @param client_port
     * Port to use for the client interface, or zero if the system
     * should decide the port.
     * @param command_port
     * Port to use for the command interface, or zero if the system
     * should decide the port.
     * @throws NullPointerException
     * If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root, int client_port, int command_port) {
        if (root == null) {
            throw new NullPointerException("root cannot be null");
        }

        this.root = root;
        storageSkeleton = new Skeleton<Storage>(Storage.class, this, new InetSocketAddress(client_port));
        commandSkeleton = new Skeleton<Command>(Command.class, this, new InetSocketAddress(command_port));
        started = false;
    }

    /**
     * Creats a storage server, given a directory on the local filesystem.
     * <p>
     * <p>
     * This constructor is equivalent to <code>StorageServer(root, 0, 0)</code>.
     * The system picks the ports on which the interfaces are made available.
     *
     * @param root Directory on the local filesystem. The contents of this
     *             directory will be accessible through the storage server.
     * @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root) {
        this(root, 0, 0);
    }

    /**
     * Starts the storage server and registers it with the given naming server.
     *
     * @param hostname      The externally-routable hostname of the local host on which
     *                      the storage server is running. This is used to ensure that the
     *                      stub which is provided to the naming server by the
     *                      <code>start</code> method carries the externally visible
     *                      hostname or address of this storage server.
     * @param naming_server Remote interface for the naming server with which the storage
     *                      server is to register.
     * @throws UnknownHostException  If a stub cannot be created for the storage server because a
     *                               valid address has not been assigned.
     * @throws FileNotFoundException If the directory with which the server was created does not
     *                               exist or is in fact a file.
     * @throws RMIException          If the storage server cannot be started, or if it cannot be
     *                               registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
            throws RMIException, UnknownHostException, FileNotFoundException {

        this.storageSkeleton.start();
        this.commandSkeleton.start();

        Storage storage = Stub.create(Storage.class, this.storageSkeleton, hostname);
        Command command = Stub.create(Command.class, this.commandSkeleton, hostname);

        Path[] paths = naming_server.register(storage, command, Path.list(this.root));

        for (Path path : paths) {
            this.delete(path);
        }

        started = true;
    }

    /**
     * Stops the storage server.
     * <p>
     * <p>
     * The server should not be restarted.
     */
    public void stop() {
        started = false;
        this.storageSkeleton.stop();
        this.commandSkeleton.stop();
        this.stopped(null);
    }

    /**
     * Called when the storage server has shut down.
     *
     * @param cause The cause for the shutdown, if any, or <code>null</code> if
     *              the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause) {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException {

        File f = file.toFile(this.root);
        if (f.isDirectory()) {
            throw new FileNotFoundException();
        }
        if (!f.exists()) {
            throw new FileNotFoundException();
        }
        return f.length();

    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length) throws FileNotFoundException, IOException {

        File f = file.toFile(this.root);
        if (f.isDirectory()) {
            throw new FileNotFoundException();
        }
        if (!f.exists()) {
            throw new FileNotFoundException();
        }

        long flen = f.length();

        if (offset < 0 || length < 0 || offset + length > flen) {
            throw new IndexOutOfBoundsException();
        }

        byte[] res = new byte[length];
        RandomAccessFile rf = new RandomAccessFile(f, "r");
        rf.seek(offset);
        rf.read(res, 0, length);
        rf.close();
        return res;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data) throws FileNotFoundException, IOException {

        if (file == null || data == null) {
            throw new NullPointerException();
        }

        File f = file.toFile(this.root);
        if (f.isDirectory()) {
            throw new FileNotFoundException();
        }
        if (!f.exists()) {
            throw new FileNotFoundException();
        }

        if (offset < 0) {
            throw new IndexOutOfBoundsException();
        }

        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(offset);
        rf.write(data, 0, data.length);
        rf.close();

    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file) {
        if (file == null) {
            throw new NullPointerException();
        }
        if (file.isRoot()) {
            return false;
        }
        File parfile = file.parent().toFile(root);
        if (!parfile.exists() && !parfile.mkdirs()) {
            return false;
        }

        File curfile = file.toFile(root);
        boolean success = false;
        try {
            success = curfile.createNewFile();
        } catch (IOException e) {
            System.out.println("IOException in StorageServer create!");
        }

        return success;
    }

    @Override
    public synchronized boolean delete(Path path) {

        if (path == null) {
            throw new NullPointerException();
        }
        if (path.isRoot()) {
            return false;
        }

        File file = path.toFile(root);

        if (!this.delete(file)) {
            return false;
        }

        file = file.getParentFile();
        while (!file.equals(root) && file.list().length == 0) {
            file.delete();
            file = file.getParentFile();
        }

        return true;
    }

    // helper function to delete files recursively
    private boolean delete(File f) {
        if (f == null) {
            throw new NullPointerException();
        }
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            boolean success = false;
            for (File file : files) {
                success = this.delete(file);
                if (!success) {
                    return false;
                }
            }
        }
        return f.delete();
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
            throws RMIException, FileNotFoundException, IOException {

        if (file == null || server == null) {
            throw new NullPointerException();
        }
        if (file.isRoot()) {
            return false;
        }

        long copysize = server.size(file);
        this.delete(file); // delete the old version
        this.create(file); // create a new version
        // read from server and write to local
        this.write(file, 0, server.read(file, 0, (int) copysize));
        return true;
    }
}
