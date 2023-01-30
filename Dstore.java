import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class Dstore {

    private File folder;
    private PrintWriter controllerOut;

    /**
     * @param port (network port to listen on)
     * @param cport (controller's port to talk to)
     * @param timeout (in milliseconds)
     * @param file_folder (where to store the data locally)
     */
    public Dstore(int port, int cport, int timeout, String file_folder) {

        new Thread(new PersistentThread(port, cport, file_folder)).start();

        try ( ServerSocket commSocket = new ServerSocket(port) ) {
            while (true) {
                Socket client = commSocket.accept();
                new Thread(new ClientThread(client, timeout)).start();
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void main(String[] args) {
        new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                Integer.parseInt(args[2]), args[3]);
    }

    /**
     * Represents the persistent connection to the Controller
     */
    private class PersistentThread implements Runnable {
        int port;
        int cport;
        String file_folder;

        public PersistentThread(int port, int cport, String file_folder) {
            this.port = port;
            this.cport = cport;
            this.file_folder = file_folder;
        }

        /**
         * Establishes initial connection with Controller
         * It then listens for communication from the Controller
         */
        @Override
        public void run() {
            try (
                    Socket dStoreSocket = new Socket(InetAddress.getLocalHost(), cport);
                    BufferedReader in = new BufferedReader(new InputStreamReader(dStoreSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(dStoreSocket.getOutputStream(), true)
            ) {
                out.println("JOIN " + port);

                // create directory if doesnt exist, wipe if does
                File fileFolder = new File(file_folder);
                folder = fileFolder;
                if (!Files.isDirectory(fileFolder.toPath())) {
                    Files.createDirectory(fileFolder.toPath());
                } else {
                    Files.walk(fileFolder.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);

                    Files.createDirectory(fileFolder.toPath());
                }

                controllerOut = out;

                while (true) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null ) {
                        // REMOVE operation
                        if (inputLine.startsWith("REMOVE")) {
                            var filename = inputLine.replace("REMOVE ", "");
                            var fileToRemove = new File(folder.getAbsoluteFile(), filename);
                            if (fileToRemove.exists()) {
                                fileToRemove.delete();
                                controllerOut.println("REMOVE_ACK " + filename);
                                System.out.println("REMOVE request of " + filename);
                                return;
                            } else {
                                controllerOut.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
                                System.err.println(filename + " doesn't exist!");
                            }
                        }

                        // rebalance LIST operation
                        if (inputLine.startsWith("LIST")) {
                            var files = folder.listFiles();
                            var fileList = new StringBuilder();

                            for (File file : files) {
                                fileList.append(file.getName() + " ");
                            }

                            controllerOut.println("LIST " + fileList);
                            System.out.println("Listing files " + fileList);
                        }

//                        // rebalance files operation
//                        if (inputLine.startsWith("REBALANCE")) {
//                            var splitString = inputLine.replace("REBALANCE ", "").split("\\s+");
//
//                            // handle sending files
//                            var noOfSends = Integer.parseInt(splitString[0]);
//                            if (noOfSends != 0) {
//                                for (int i = 1; i <= noOfSends; i++) {
//                                    var fileToSend = splitString[i];
//                                    var noOfPortsTo = Integer.parseInt(splitString[i+1]);
//
//                                    for (int p = 1; p <= noOfPortsTo; p++) {
//                                        var portToSendTo = splitString[i+1+p];
//
//                                    }
//                                }
//
//                            }
//                        }

                    }
                }
            } catch (Exception e) { e.printStackTrace(); }
        }

    }

    /**
     * Represents a connection to a client
     */
    private class ClientThread implements Runnable {
        Socket client;
        int fileSize;
        File fileToStore;
        int timeout;

        public ClientThread(Socket client, int timeout) {
            this.client = client;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try (
                    BufferedReader commIn = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter commOut = new PrintWriter(client.getOutputStream(), true)
            ) {
                client.setSoTimeout(timeout);

                String inputLine;
                while ((inputLine = commIn.readLine()) != null) {
                    // STORE operation
                    if (inputLine.startsWith("STORE")) {
                        var fileDetails = inputLine.replace("STORE ", "").split(" ");
                        fileToStore = new File(folder.getAbsoluteFile(), fileDetails[0]);
                        fileSize = Integer.parseInt(fileDetails[1]);
                        commOut.println("ACK");

                        // read file data
                        try ( BufferedInputStream dataIn = new BufferedInputStream(client.getInputStream()) ) {
                            System.out.println("Reading File...");

                            byte[] dataLine = dataIn.readNBytes(fileSize);
                            Path filePath = Paths.get(fileToStore.getAbsolutePath());
                            Files.write(filePath, dataLine);

                            System.out.println("Stored " + fileToStore.getName() + " in " + folder.getName());

                            controllerOut.println("STORE_ACK " + fileToStore.getName());
                            return;
                        } catch (Exception e) {
                            System.err.println("Store failed");
                            e.printStackTrace();
                        }
                    }

                    if (inputLine.startsWith("LOAD_DATA")) {
                        var fileName = inputLine.replace("LOAD_DATA ", "");
                        var file = new File(folder.getAbsoluteFile(), fileName);
                        if (file.exists()) {
                            var fileData = Files.readAllBytes(file.toPath());

                            try ( BufferedOutputStream dataOut = new BufferedOutputStream(client.getOutputStream()) ) {
                                dataOut.write(fileData);
                                System.out.println("LOAD of " + fileName + " is complete!");
                                return;
                            } catch (Exception e) {
                                System.err.println("Load failed");
                                e.printStackTrace();
                            }
                        } else {
                            client.close();
                        }
                    }
                }
            } catch (Exception e) { e.printStackTrace(); }
        }
    }

}