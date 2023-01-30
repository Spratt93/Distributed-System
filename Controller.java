import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {

    // keeps track of stored files / filename -> lifecycle
    private ConcurrentHashMap<String, String> index = new ConcurrentHashMap<>();
    // keeps track of the no. of Dstores
    private AtomicInteger availableDstores = new AtomicInteger(0);
    // records communication channel for each dstore
    private ConcurrentHashMap<String, BufferedReader> dStoreInSockets = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, PrintWriter> dStoreOutSockets = new ConcurrentHashMap<>();

    // used to handle edge case of reloading
    private ConcurrentHashMap<String, Integer> loadTries = new ConcurrentHashMap<>();
    // records the files that each dstore stores
    private ConcurrentHashMap<String, List<String>> distribution = new ConcurrentHashMap<>();
    // keeps track of file sizes
    private ConcurrentHashMap<String, Integer> sizes = new ConcurrentHashMap<>();
    // for repeated rebalancing
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final String removeInProgress = "RIP";
    private final String storeInProgress = "SIP";
    private final String storeComplete = "SC";

    private final int timeout;
    private final int r;

    /**
     * @param cport           (network port to listen on)
     * @param r               (replication factor)
     * @param timeout         (in milliseconds)
     * @param rebalancePeriod (in seconds)
     */
    public Controller(int cport, int r, int timeout, int rebalancePeriod) {

        this.timeout = timeout;
        this.r = r;

        Runnable doRebalance = this::rebalance;
        scheduler.scheduleAtFixedRate(doRebalance, rebalancePeriod, rebalancePeriod, TimeUnit.SECONDS);

        try (ServerSocket serverSocket = new ServerSocket(cport)) {
            // so that the controller can be left running
            while (true) {
                // waits until a client requests a connection
                // returns a Socket instance to handle the connection
                Socket client = serverSocket.accept();
                new Thread(new ControllerThread(client)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Ensures that files are replicated 'r' times
     * Ensures even file distribution
     * Locked because only 1 rebalance should run at a given time
     */
    public synchronized void rebalance() {
        System.out.println("Rebalancing...");
        ConcurrentHashMap<String, List<String>> rebalanceResults = new ConcurrentHashMap<>();
        // dstore -> files to send TO that dstore
        ConcurrentHashMap<String, List<String>> filesToSend = new ConcurrentHashMap<>();
        // dstore -> files to send FROM that dstore
        ConcurrentHashMap<String, List<String>> fromDstore = new ConcurrentHashMap<>();
        // dstore -> files to remove FROM that dstore
        ConcurrentHashMap<String, List<String>> filesToRemove = new ConcurrentHashMap<>();

        // await any pending store or remove operations
        while (index.containsValue(storeInProgress) || index.containsValue(removeInProgress)) {
            // wait for pending stores or removes to complete
        }

        // get the files from each dStore
        // this part should handle the failures of dstores
        getNewDistribution(rebalanceResults);

        // Part 1. of Rebalance
        // ensureCorrectReplication(rebalanceResults, filesToSend, fromDstore);

        // part 2. of Rebalance
        // ensureEvenDistribution(rebalanceResults, filesToSend, fromDstore, filesToRemove);

        // send the message to all the dstores
        // sendRebalanceMessages(rebalanceResults, filesToSend, fromDstore, filesToRemove);
    }

    /**
     * Queries the current status of dStores
     * Returns the updated file distribution
     * @param rebalanceResults
     */
    public void getNewDistribution(ConcurrentHashMap<String, List<String>> rebalanceResults) {
        System.out.println("Checking each DStore status...");

        for (String dStore : dStoreOutSockets.keySet()) {
            try {
                dStoreOutSockets.get(dStore).println("LIST");
                var latch = new CountDownLatch(1);
                new Thread(new AckThread(dStoreInSockets.get(dStore), latch, dStore, rebalanceResults)).start();
                // handles unresponsive dStore
                if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    System.err.println("TIMEOUT FOR DSTORE " + dStore);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Ensures that each file is replicated 'r' times
     * @param rebalanceResults
     * @param filesToSend
     * @param fromDstore
     */
    public void ensureCorrectReplication(ConcurrentHashMap<String, List<String>> rebalanceResults,
                                         ConcurrentHashMap<String, List<String>> filesToSend,
                                         ConcurrentHashMap<String, List<String>> fromDstore) {

        for (String file : index.keySet()) {
            int replicated = 0;
            String dStoreFrom = "";

            for (String dStore : rebalanceResults.keySet()) {
                if (rebalanceResults.get(dStore).contains(file)) {
                    replicated++;
                    // should also ensure smallest dstore is selected
                    // because last in the list will be the newest
                    dStoreFrom = dStore;

                }
            }

            if (replicated < r) {
                var diff = r - replicated;
                System.out.println("Replicating " + file + " " + diff + " times...");

                // sort the dStores in ascending order
                // this ensures the smallest dStore is picked first
                var list = new ArrayList<>(rebalanceResults.entrySet());
                list.sort((o1, o2) -> {
                    if (o1.getValue().size() < o2.getValue().size()) {
                        return -1;
                    }
                    if (o1.getValue().size() > o2.getValue().size()) {
                        return 1;
                    } else {
                        return 0;
                    }
                });

                // add the file to dstores that it isn't in
                for (int i = 0; i < list.size(); i++) {
                    String dStore = list.get(i).getKey();

                    if (diff <= 0) {
                        break;
                    }

                    if (!rebalanceResults.get(dStore).contains(file)) {
                        if (filesToSend.containsKey(dStore)) {
                            var fileList = filesToSend.get(dStore);
                            fileList.add(file);
                            filesToSend.replace(dStore, fileList);
                        } else {
                            ArrayList<String> fileList = new ArrayList<>();
                            fileList.add(file);
                            filesToSend.put(dStore, fileList);
                        }

                        // update rebalance results for even dist. calculations
                        var prev = rebalanceResults.get(dStore);
                        prev.add(file);
                        rebalanceResults.replace(dStore, prev);

                        // and update dist. for future store and remove operations
                        var newDist = distribution.get(dStore);
                        newDist.add(file);
                        distribution.replace(dStore, newDist);

                        if (fromDstore.containsKey(dStoreFrom)) {
                            var filesFrom = fromDstore.get(dStoreFrom);
                            filesFrom.add(file);
                            fromDstore.replace(dStoreFrom, filesFrom);
                        } else {
                            ArrayList<String> filesFrom = new ArrayList<>();
                            filesFrom.add(file);
                            fromDstore.put(dStoreFrom, filesFrom);
                        }
                        diff--;
                    }
                }
            } else {
                System.out.println(file + " is replicated correctly");
            }
        }
    }

    /**
     * Ensures even distribution of files across dStores
     * @param rebalanceResults
     * @param filesToSend
     * @param fromDstore
     * @param filesToRemove
     */
    public void ensureEvenDistribution(ConcurrentHashMap<String, List<String>> rebalanceResults,
                                       ConcurrentHashMap<String, List<String>> filesToSend,
                                       ConcurrentHashMap<String, List<String>> fromDstore,
                                       ConcurrentHashMap<String, List<String>> filesToRemove) {

        // dstore -> no. files to remove
        ConcurrentHashMap<String, Integer> removeCount = new ConcurrentHashMap<>();

        for (String dStore : rebalanceResults.keySet()) {
            // stops a dstore being chosen
            List<String> prevChosen = new ArrayList<>();

            // a dstore with too few files
            var noOfActiveStores = rebalanceResults.size();
            while (rebalanceResults.get(dStore).size() < Math.floorDiv(r * index.size(), noOfActiveStores)) {
                System.err.println("Uneven file distribution in DStore " + dStore);

                // sorts dStores in descending order in terms of size
                var list = new ArrayList<>(rebalanceResults.entrySet());
                list.sort((o1, o2) -> {
                    if (o1.getValue().size() < o2.getValue().size()) {
                        return 1;
                    }
                    if (o1.getValue().size() > o2.getValue().size()) {
                        return -1;
                    } else {
                        return 0;
                    }
                });

                // choose the dstore with the most files that hasn't already been chosen
                String storeChosen = "";
                for (int i = 0; i < list.size(); i++) {
                    if (!prevChosen.contains(list.get(i).getKey())) {
                        storeChosen = list.get(i).getKey();
                        prevChosen.add(list.get(i).getKey());
                        break;
                    }
                }

                System.out.println("DStore chosen to send from: " + storeChosen);

                // choose the first different file
                // can't have same file in the same dstore more than once
                int tried = 0;
                while (rebalanceResults.get(dStore).contains(rebalanceResults.get(storeChosen).get(tried))) {
                    tried++;
                }
                var fileChosen = rebalanceResults.get(storeChosen).get(tried);

                // remove that file from the large dstore
                if (filesToRemove.containsKey(storeChosen)) {
                    var lst = filesToRemove.get(storeChosen);
                    lst.add(fileChosen);
                    filesToRemove.replace(storeChosen, lst);

                } else {
                    var lst = new ArrayList<String>();
                    lst.add(fileChosen);
                    filesToRemove.put(storeChosen, lst);
                }

                // increment the counter
                if (removeCount.containsKey(storeChosen)) {
                    var curr = removeCount.get(storeChosen) + 1;
                    removeCount.replace(storeChosen, curr);
                } else {
                    removeCount.put(storeChosen, 1);
                }

                // send the file to the dstore in need
                if (filesToSend.containsKey(dStore)) {
                    var fileList = filesToSend.get(dStore);
                    fileList.add(fileChosen);
                    filesToSend.replace(dStore, fileList);
                } else {
                    var fileList = new ArrayList<String>();
                    fileList.add(fileChosen);
                    filesToSend.put(dStore, fileList);
                }

                if (fromDstore.containsKey(storeChosen)) {
                    var fileList = fromDstore.get(storeChosen);
                    fileList.add(fileChosen);
                    fromDstore.replace(storeChosen, fileList);
                } else {
                    var fileList = new ArrayList<String>();
                    fileList.add(fileChosen);
                    fromDstore.put(storeChosen, fileList);
                }

                // update rebalance results
                var old = rebalanceResults.get(storeChosen);
                old.remove(fileChosen);
                rebalanceResults.replace(storeChosen, old);

                var upd = rebalanceResults.get(dStore);
                upd.add(fileChosen);
                rebalanceResults.replace(dStore, upd);

                // and update dist. for future store and remove operations
                var changeDist = distribution.get(storeChosen);
                changeDist.remove(fileChosen);
                distribution.replace(storeChosen, changeDist);

                var newDist = distribution.get(dStore);
                newDist.add(fileChosen);
                distribution.replace(dStore, newDist);

                System.out.println("Moving " + fileChosen + " from " + storeChosen + " to dStore " + dStore);
            }
        }
    }

    /**
     * Constructs the rebalance messages
     * @param rebalanceResults
     * @param filesToSend
     * @param fromDstore
     * @param filesToRemove
     */
    public void sendRebalanceMessages(ConcurrentHashMap<String, List<String>> rebalanceResults,
                                      ConcurrentHashMap<String, List<String>> filesToSend,
                                      ConcurrentHashMap<String, List<String>> fromDstore,
                                      ConcurrentHashMap<String, List<String>> filesToRemove) {

        for (String dStore : rebalanceResults.keySet()) {
            // handles removes
            StringBuilder removeMessage = new StringBuilder();
            if (filesToRemove.containsKey(dStore)) {
                var removeFiles = filesToRemove.get(dStore);
                removeMessage.append(removeFiles.size() + " ");
                for (String file : removeFiles) {
                    removeMessage.append(file + " ");
                }
            } else {
                removeMessage.append("0");
            }

            // handles send message
            var toSendMessage = new StringBuilder();
            // file -> dstores to send to
            HashMap<String, String> storeList = new HashMap<>();
            // file -> no. of dstores to send to
            HashMap<String, Integer> storeCount = new HashMap<>();

            // handles sends
            if (fromDstore.containsKey(dStore)) {
                // files to be sent from dStore
                var sendFiles = fromDstore.get(dStore);

                for (String file : sendFiles) {
                    StringBuilder stores = new StringBuilder();

                    int storeCounter = 0;
                    for (String store : filesToSend.keySet()) {
                        if (filesToSend.get(store).contains(file)) {
                            stores.append(store + " ");
                            storeCounter++;
                        }
                    }

                    storeCount.put(file, storeCounter);
                    storeList.put(file, String.valueOf(stores));
                }

                // total no. of files to send
                toSendMessage.append(sendFiles.size() + " ");

                for (String file : storeList.keySet()) {
                    // filename
                    toSendMessage.append(file + " ");
                    // no. of sends
                    toSendMessage.append(storeCount.get(file) + " ");
                    // dstores to send to
                    toSendMessage.append(storeList.get(file));
                }
            } else {
                toSendMessage.append("0 ");
            }

            System.out.println("REBALANCE " + toSendMessage + removeMessage);
            dStoreOutSockets.get(dStore).println("REBALANCE " + toSendMessage + removeMessage);
        }
    }

    /**
     * Handles concurrent STORE operation
     *
     * @param out
     * @param inputLine
     */
    public void store(PrintWriter out, String inputLine) {
        var input = inputLine.replace("STORE ", "").split(" ");
        var fileToStore = input[0];
        var fileSize = input[1];

        // ensures 2 clients cannot store the same file at the same time
        synchronized (this) {
            if (index.containsKey(fileToStore)) {
                out.println("ERROR_FILE_ALREADY_EXISTS");
                System.err.println(fileToStore + " already exists");
                return;
            }

            index.put(fileToStore, storeInProgress);
        }

        try {
            System.out.println("STORE of " + fileToStore + " in progress...");
            sizes.put(fileToStore, Integer.valueOf(fileSize));

            // equal distribution algorithm
            var list = new ArrayList<>(distribution.entrySet());
            list.sort((o1, o2) -> {
                if (o1.getValue().size() < o2.getValue().size()) {
                    return -1;
                }
                if (o1.getValue().size() > o2.getValue().size()) {
                    return 1;
                } else {
                    return 0;
                }
            });
            // take r ports
            var ports = new StringBuilder();
            var storeLatch = new CountDownLatch(r);
            for (int i = 0; i < r; i++) {
                var port = list.get(i).getKey();
                ports.append(port + " ");

                // update the map
                List<String> updatedFiles = list.get(i).getValue();
                updatedFiles.add(fileToStore);
                distribution.replace(list.get(i).getKey(), updatedFiles);

                new Thread(new AckThread(dStoreInSockets.get(list.get(i).getKey()), storeLatch)).start();
            }

            System.out.println("Chose ports " + ports + "for storage");
            out.println("STORE_TO " + ports);

            // if controller doesn't receive all of the ACKS
            // treat the file as if it didn't store
            if (!storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                System.err.println("NOT all ACKS received - file removed");
                index.remove(fileToStore);
                sizes.remove(fileToStore);
                for (String store : distribution.keySet()) {
                    if (distribution.get(store).contains(fileToStore)) {
                        var upd = distribution.get(store);
                        upd.remove(fileToStore);
                        distribution.replace(store, upd);
                    }
                }
            } else {
                index.replace(fileToStore, storeComplete);
                System.out.println("STORE of " + fileToStore + " is complete!");
                out.println("STORE_COMPLETE");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Handles concurrent remove operation
     *
     * @param out
     * @param inputLine
     */
    public void remove(PrintWriter out, String inputLine) throws IOException, InterruptedException {
        var filename = inputLine.replace("REMOVE ", "");

        // ensures 2 clients cannot remove the same file at the same time
        synchronized (this) {
            if (!index.containsKey(filename) || !index.get(filename).equals(storeComplete)) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                System.err.println(filename + " does not exist");
                return;
            }

            System.out.println("REMOVE of " + filename + " is in progress...");
            index.replace(filename, removeInProgress);
        }

        var removeLatch = new CountDownLatch(r);
        for (String dstore : distribution.keySet()) {
            if (distribution.get(dstore).contains(filename)) {
                System.out.println("REMOVE " + filename + " from port " + dstore);
                distribution.get(dstore).remove(filename);
                sizes.remove(filename);

                dStoreOutSockets.get(dstore).println("REMOVE " + filename);

                new Thread(new AckThread(dStoreInSockets.get(dstore), removeLatch)).start();
            }
        }

        removeLatch.await(timeout, TimeUnit.MILLISECONDS);
        index.remove(filename);
        loadTries.remove(filename);
        System.out.println("REMOVE of " + filename + " is complete!");
        out.println("REMOVE_COMPLETE");

    }

    /**
     * Handles LOAD operation
     *
     * @param out
     * @param inputLine
     */
    public void load(PrintWriter out, String inputLine) {
        var filename = inputLine.replace("LOAD ", "");
        if (index.containsKey(filename) && index.get(filename).equals(storeComplete)) {
            System.out.println("LOAD of " + filename + " in progress...");

            for (String dStore : distribution.keySet()) {
                if (distribution.get(dStore).contains(filename)) {
                    System.out.println("LOAD from port " + dStore);
                    var size = sizes.get(filename);
                    out.println("LOAD_FROM " + dStore + " " + size);
                    break;
                }
            }
        } else {
            out.println("ERROR_FILE_DOES_NOT_EXIST");
            System.err.println(filename + " does not exist" + index.get(filename));
        }
    }

    /**
     * Handles LIST operation
     *
     * @param out
     */
    public void list(PrintWriter out) {
        System.out.println("LIST in progress...");
        var files = new StringBuilder();
        for (String f : index.keySet()) {
            if (index.get(f).equals(storeComplete)) {
                files.append(f + " ");
            }
        }
        out.println("LIST " + files);
        System.out.println("LIST complete!");
    }

    /**
     * Handles situation where client cannot connect or receive data from the dstore
     * Controller selects another instead
     *
     * @param out
     * @param inputLine
     */
    public void reload(PrintWriter out, String inputLine) {
        var filename = inputLine.replace("RELOAD ", "");
        System.out.println("RELOAD of " + filename + " in progress...");

        if (loadTries.containsKey(filename)) {

            if (loadTries.get(filename) < r) {
                // increment the no. of attempts if exists
                loadTries.replace(filename, loadTries.get(filename) + 1);
            } else {
                // none of the dstores work
                out.println("ERROR_LOAD");
                System.err.println("Error-load, none of the DStores will load");
            }

        } else {
            loadTries.put(filename, 1);
        }

        int triedCounter = 0;
        for (String dStore : distribution.keySet()) {
            if (distribution.get(dStore).contains(filename) && triedCounter == loadTries.get(filename)) {
                System.err.println("RELOAD " + filename + " from port " + dStore);
                out.println("LOAD_FROM " + dStore + " " + sizes.get(filename));
                break;
            }

            if (distribution.get(dStore).contains(filename)) {
                triedCounter++;
            }
        }
    }

    /**
     * Handles JOIN operation
     *
     * @param in
     * @param inputLine
     */
    public void join(BufferedReader in, PrintWriter out, String inputLine) {
        var port = inputLine.replace("JOIN ", "");
        System.out.println("Dstore added at port: " + port);
        availableDstores.getAndIncrement();
        dStoreInSockets.put(port, in);
        dStoreOutSockets.put(port, out);
        distribution.put(port, new ArrayList<>());
        rebalance();
    }

    public static void main(String[] args) {
        var arg0 = Integer.parseInt(args[0]);
        var arg1 = Integer.parseInt(args[1]);
        var arg2 = Integer.parseInt(args[2]);
        var arg3 = Integer.parseInt(args[3]);
        new Controller(arg0, arg1, arg2, arg3);
    }

    /**
     * Every time a connection is made with the controller
     * a new thread is created
     */
    private class ControllerThread implements Runnable {

        Socket client;
        boolean isDstore = false;

        public ControllerThread(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            try (
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true)
            ) {
                String inputLine;

                while ((inputLine = in.readLine()) != null) {

                    // DSTORE thread
                    if (inputLine.startsWith("JOIN")) {
                        join(in, out, inputLine);
                        isDstore = true;
                        // keep connection open
                        while (true) {

                        }
                    }

                    // CLIENT thread
                    // STORE operation
                    if (inputLine.startsWith("STORE")) {
                        if (availableDstores.get() >= r) {
                            store(out, inputLine);
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            System.err.println("Not enough Dstores");
                        }
                    }

                    // LOAD operation
                    if (inputLine.startsWith("LOAD")) {
                        if (availableDstores.get() >= r) {
                            load(out, inputLine);
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            System.err.println("Not enough Dstores");
                        }
                    }
                    // If there is a client-side LOAD failure
                    if (inputLine.startsWith("RELOAD")) {
                        reload(out, inputLine);
                    }

                    // REMOVE operation
                    if (inputLine.startsWith("REMOVE")) {
                        if (availableDstores.get() >= r) {
                            remove(out, inputLine);
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            System.err.println("Not enough Dstores");
                        }
                    }

                    // Client LIST operation
                    if (inputLine.equals("LIST") && !isDstore) {
                        if (availableDstores.get() >= r) {
                            list(out);
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Thread that handles waiting for acknowledgements
     * This is due to the fact that the Controller has to speak to both the Dstore and Client
     */
    private class AckThread implements Runnable {

        BufferedReader dStoreIn;
        CountDownLatch latch;
        String dStore;
        ConcurrentHashMap<String, List<String>> rebalanceResults;

        /**
         * Default constructor
         * Used for STORE and REMOVE acknowledgements
         *
         * @param dStoreIn
         * @param latch
         */
        public AckThread(BufferedReader dStoreIn, CountDownLatch latch) {
            this.dStoreIn = dStoreIn;
            this.latch = latch;
        }

        /**
         * Constructor for rebalance threads
         *
         * @param dStoreIn
         * @param latch
         * @param dStore
         * @param rebalanceResults
         */
        public AckThread(BufferedReader dStoreIn, CountDownLatch latch, String dStore, ConcurrentHashMap<String, List<String>> rebalanceResults) {
            this.dStoreIn = dStoreIn;
            this.latch = latch;
            this.dStore = dStore;
            this.rebalanceResults = rebalanceResults;
        }

        @Override
        public void run() {

            try {
                String inputLine;
                while ((inputLine = dStoreIn.readLine()) != null) {
                    if (inputLine.startsWith("STORE_ACK")) {
                        System.out.println("RECEIVED STORE_ACK");
                        latch.countDown();
                        return;
                    }

                    if (inputLine.startsWith("REMOVE_ACK")) {
                        System.out.println("RECEIVED REMOVE_ACK");
                        latch.countDown();
                        return;
                    }

                    if (inputLine.startsWith("LIST")) {
                        // handle no files case
                        var noFiles = inputLine.replace("LIST ", "");
                        if (noFiles.equals("")) {
                            ArrayList<String> empty = new ArrayList<>();
                            rebalanceResults.put(dStore, empty);
                            latch.countDown();
                            return;
                        }

                        var files = inputLine.replace("LIST ", "").trim().split("\\s+");
                        List<String> fileList = new ArrayList<>();
                        fileList.addAll(Arrays.asList(files));
                        rebalanceResults.put(dStore, fileList);
                        latch.countDown();
                        return;
                    }
                }
            } catch (Exception e) {
                if (dStore != null) {
                    System.err.println("DStore at port " + dStore + " is disconnected");
                    dStoreOutSockets.remove(dStore);
                    dStoreInSockets.remove(dStore);
                    availableDstores.getAndDecrement();
                    distribution.remove(dStore);
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

}