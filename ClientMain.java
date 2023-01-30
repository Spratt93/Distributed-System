import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ClientMain {
	
	public static void main(String[] args) throws Exception{
		
		final int cport = Integer.parseInt(args[0]);
		int timeout = Integer.parseInt(args[1]);
		
		/*// this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
		
		// this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
		File uploadFolder = new File("to_store");
		if (!uploadFolder.exists())
			throw new RuntimeException("to_store folder does not exist");
		
		// launch a single client
		testClient(cport, timeout, downloadFolder, uploadFolder);
		
		// launch a number of concurrent clients, each doing the same operations
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					test2Client(cport, timeout, downloadFolder, uploadFolder);
				}
			}.start();
		}*/
		tomsTest(cport, timeout);
	}
	
	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());
			
			File fileList[] = uploadFolder.listFiles();
			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {					
					client.store(fileToStore);
				} catch (Exception e) {
					System.out.println("Error storing file " + fileToStore);
					e.printStackTrace();
				}
			}
			
			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}
			
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}
	
	public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
		
			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }				
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();
		
		System.out.println("Ok, " + list.length + " files:");
		int i = 0; 
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);
		
		return list;
	}

	public static void tomsTest(int cport, int timeout) {
		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

		// this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
		File uploadFolder = new File("to_store");
		if (!uploadFolder.exists())
			throw new RuntimeException("to_store folder does not exist");

		Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

		try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

		File fileList[] = uploadFolder.listFiles();
		try {
			client.store(fileList[0]);
			client.store(fileList[1]);
			//client.load(fileList[0].getName(), downloadFolder);
			//client.load(fileList[0].getName(), downloadFolder);
			//list(client);
			//client.remove(fileList[0].getName());
		} catch(IOException e) { e.printStackTrace(); }

		/*new Thread() {
			public void run() {
				try {
					Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
					client.connect();

					File fileList[] = uploadFolder.listFiles();
					File fileToStore0 = fileList[0];
					client.store(fileToStore0);
					client.remove(fileToStore0.getName());
				} catch (Exception e) { e.printStackTrace(); }

			}
		}.start();

		new Thread() {
			public void run() {
				try {
					Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
					client.connect();

					File fileList[] = uploadFolder.listFiles();
					File fileToStore1 = fileList[1];
					client.store(fileToStore1);
					client.load(fileToStore1.getName(), downloadFolder);
				} catch (Exception e) { e.printStackTrace(); }

			}
		}.start();

		new Thread() {
			public void run() {
				try {
					Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
					client.connect();

					File fileList[] = uploadFolder.listFiles();
					File fileToStore1 = fileList[1];
					list(client);
					client.remove(fileToStore1.getName());
				} catch (Exception e) { e.printStackTrace(); }

			}
		}.start();*/

	}
	
}
