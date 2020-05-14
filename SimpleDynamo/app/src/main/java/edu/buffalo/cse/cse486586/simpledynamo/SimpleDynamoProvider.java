package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;

import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String [] portList = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	HashMap<String, String> portToHashes =  new HashMap<String, String>();
	HashMap<String, String> hashToPort = new HashMap<String, String>();
	HashMap<String, String> firstSuccMap = new HashMap<String, String>();
	HashMap<String, String> secondSuccMap = new HashMap<String, String>();
	ArrayList<String> insertedKeyList = new ArrayList<String>();
	String firstPredNode = null; // First Predecessor hash and port
	String firstPredPort = null;
	String firstSuccNode = null; // First successor hash and port
	String firstSuccPort = null;
	String secondSuccNode = null; // Second successor hash and port
	String secondSuccPort = null;
	String secondPredNode = null; // Second predecessor hash and port
	String secondPredPort = null;
	String node = ""; // My own hash and port
	String nodePort = "";

	ReentrantLock insertLock = new ReentrantLock();
	ReentrantLock queryLock = new ReentrantLock();
	ReentrantLock deleteLock = new ReentrantLock();
	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	public Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public void starDeleteQuery() {
		Log.e("starDeleteQuery","Entering starDeleteQuery");
		for(int i = 0; i < portList.length; i++) {
			try {
				String remotePort = portList[i];
				if (remotePort != nodePort) { // Not my port
					Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					String msgToSend = "DELETE*" + "\n";

					OutputStream out = s.getOutputStream();
					DataOutputStream dos = new DataOutputStream(out);
					dos.writeBytes(msgToSend);
					dos.flush();
				}
			} catch (UnknownHostException e) {
				Log.e("starDeleteQuery", "ClientSide UnknownHostException");
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("starDeleteQuery", "ClientSide IOException");
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("starDeleteQuery", "ClientSide Generic Exception");
				e.printStackTrace();
			}
		}
		return;
	}

	public void deleteImm(Uri uri, String key) {
		try {
			String filename = genHash(key);
			boolean res = getContext().deleteFile(filename);
			Log.e("deleteImm", "Deleted successfully" + filename);
			insertedKeyList.remove(key);
		} catch(NoSuchAlgorithmException e) {
			Log.e("deleteImm", "No such algorithm");
			e.printStackTrace();
		}
		return;
	}

	public void deleteReplica(String msg) {
		sendUpdate(firstSuccPort, msg);
		sendUpdate(secondSuccPort, msg);
		return;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		deleteLock.lock();
		Log.e("delete", selection);
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);

		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e("delete", "Delete selection is @");
				for(int i = 0; i < insertedKeyList.size(); i++) {
					String filename = genHash(insertedKeyList.get(i));
					boolean res = getContext().deleteFile(filename);

					Log.e("delete", "Deleted successfully :" + filename);
					//Delete from my replicas
					String msg = "DELREP" + "-" + selection;
					deleteReplica(msg);
				}
				insertedKeyList.clear();
				if(selection.compareTo("*") == 0)
				{
					starDeleteQuery();
				}
			} else {
				String filename = genHash(selection);
				String port = checkPartition(filename);
				if(nodePort.compareTo(port) == 0)
				{
					boolean res = getContext().deleteFile(filename);
					Log.e("delete", "Deleted successfully" + filename);
					insertedKeyList.remove(selection);
					//Delete from my replicas
					String msg = "DELREP" + "-" + selection;
					deleteReplica(msg);
				} else { // Forward to the delete command to the node where the key belongs
					/*String msg = "DELETE" + "-" + selection;
					if(1 == sendUpdate(port, msg)) {
						Log.e("delete", "Failed to delete from the right partition, node is dead");
						*//* Delete contents from the replicas */
						String msg = "DELREP" + "-" + selection;
						sendUpdate(port, msg);
						Log.e("delete", "Successfully sent delete to the right partition:" + port);
						/* Key deosn't belong to me, but I am the first replica */
						if(firstSuccMap.get(port).compareTo(nodePort) == 0) {
							boolean res = getContext().deleteFile(filename);
							Log.e("delete", "Deleted successfully" + filename);
							insertedKeyList.remove(selection);
						} else {
							sendUpdate(firstSuccMap.get(port), msg);
							Log.e("delete", "Successfully sent delete to the replica:" + firstSuccMap.get(port));
						}

						/* Key deosn't belong to me but I am the second replica */
						if(secondSuccMap.get(port).compareTo(nodePort) == 0) {
							boolean res = getContext().deleteFile(filename);
							Log.e("delete", "Deleted successfully" + filename);
							insertedKeyList.remove(selection);
						} else {
							sendUpdate(secondSuccMap.get(port), msg);
							Log.e("delete", "Successfully sent delete to the right partition:" + secondSuccMap.get(port));
						}
					/*} else {
						Log.e("delete", "Successfully send delete to the right partition");
					}*/
				}
			}
		} catch(Exception e) {
			Log.e("delete", "Exception in deleting");
		} finally {
			deleteLock.unlock();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void populateHash() {
		try {
			portToHashes.put("11108", genHash("5554"));
			portToHashes.put("11112", genHash("5556"));
			portToHashes.put("11116", genHash("5558"));
			portToHashes.put("11120", genHash("5560"));
			portToHashes.put("11124", genHash("5562"));

			hashToPort.put(genHash("5554"), "11108");
			hashToPort.put(genHash("5556"), "11112");
			hashToPort.put(genHash("5558"), "11116");
			hashToPort.put(genHash("5560"), "11120");
			hashToPort.put(genHash("5562"), "11124");
		} catch(NoSuchAlgorithmException e) {
			Log.e("populateHash", "No such algorithm exception");
			e.printStackTrace();
		}
	}

	public void populateFirstSuccMap() {
		firstSuccMap.put("11108", "11116");
		firstSuccMap.put("11116", "11120");
		firstSuccMap.put("11120", "11124");
		firstSuccMap.put("11124", "11112");
		firstSuccMap.put("11112", "11108");
	}

	public void populateSecondSuccMap() {
		secondSuccMap.put("11108", "11120");
		secondSuccMap.put("11116", "11124");
		secondSuccMap.put("11120", "11112");
		secondSuccMap.put("11124", "11108");
		secondSuccMap.put("11112", "11116");
	}

	public String checkPartition(String keyhash) {
		if(keyhash.compareTo(portToHashes.get("11124")) > 0 && keyhash.compareTo(portToHashes.get("11112")) <= 0) {
			//Lies between 11124 and 11112
			return "11112";
		} else if(keyhash.compareTo(portToHashes.get("11112")) > 0 && keyhash.compareTo(portToHashes.get("11108")) <= 0 ) {
			// Lies between 11112 and 11108
			return "11108";
		} else if(keyhash.compareTo(portToHashes.get("11108")) > 0 && keyhash.compareTo(portToHashes.get("11116")) <= 0 ) {
			// Lies between 11108 and 11116
			return "11116";
		} else if(keyhash.compareTo(portToHashes.get("11116")) > 0 && keyhash.compareTo(portToHashes.get("11120")) <= 0 ) {
			// Lies between 11116 and 11120
			return "11120";
		} /* else if(keyhash.compareTo(portToHashes.get("11120")) <= 0 && keyhash.compareTo(portToHashes.get("11124")) > 0 ) {
			// Lies between 11120 and 11124
			return "11124";
		} */else {
			return "11124";
		}
	}

	public int sendUpdate(String port, String msg) {
		int retval = 0;
		Log.e("sendUpdate","Send Update message " + msg + " to port " + port);
		try {
			Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			s.setSoTimeout(2000);
			String msgToSend = msg + "\n";

			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeBytes(msgToSend);
			dos.flush();

            /*// Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor */
		} /*catch(SocketTimeoutException e) {
			Log.e("sendUpdate", " Socket Timeout Exception");
			retval = 1;
		} catch(UnknownHostException e) {
			Log.e("sendUpdate", "UnknownHostException");
			retval = 1;
		} catch(IOException e) {
			Log.e("sendUpdate", "IOException");
			retval = 1;
		} */catch(Exception e) {
			Log.e("sendUpdate", "Generic Exception");
			retval = 1;
			e.printStackTrace();
		}
		return retval;
	}

	public void replicateInsert(String msg) {
		sendUpdate(firstSuccPort, msg);
		sendUpdate(secondSuccPort, msg);
	}

	public void insertImm(Uri uri, String key, String value) {
		Log.e("insertImm", "Entering insert for key: " + key
				+ " value:" + value);
		try {
			String filename = genHash(key);
			Log.e("insertImm", "Creating file " + filename);
			String string = value;
			FileOutputStream outputStream;

				outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.close();
				insertedKeyList.add(key);
				Log.e("insertImm", "Successfully inserted " + value);
		} catch(Exception e) {
			Log.e("insertImm", "File write failed " + e.toString());
			e.printStackTrace();
		}
		return;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		insertLock.lock();
		Log.e("insert", "Entering insert for key: " + values.get("key").toString()
				+ " value:" + values.get("value").toString());
		try {
			String filename = genHash(values.get("key").toString());
			String port = checkPartition(filename);
			if(port.compareTo(nodePort) == 0) {
				Log.e("insert", "Creating file " + filename);
				String string = values.get("value").toString();
				FileOutputStream outputStream;

				outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.close();
				insertedKeyList.add(values.get("key").toString());
				Log.e("insert", "Successfully inserted " + values.get("value").toString());

				// Replicate to two successors
				String msg = "REP" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
				replicateInsert(msg);
			} else { // Sends it to the right partition node and also replicate for that node
				/*String msg = "INSERT" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
				if(1 == sendUpdate(port, msg)) {
					Log.e("insert", "Failed to send to the right partition, node is dead");
				*/	/* Replicate on behalf of the failed node*/
					String msg = "REP" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
					sendUpdate(port, msg);
					Log.e("insert", "Successfully sent to the right partition:hash: " + filename + "port: " + port);
					/* Key doesn't belong to me, but I am the first replica */
					if(firstSuccMap.get(port).compareTo(nodePort) == 0) {
						Log.e("insert", "Creating file " + filename);
						String string = values.get("value").toString();
						FileOutputStream outputStream;

						outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
						outputStream.write(string.getBytes());
						outputStream.close();
						insertedKeyList.add(values.get("key").toString());
						Log.e("insert", "Successfully inserted " + values.get("value").toString());

					} else {
						sendUpdate(firstSuccMap.get(port), msg);
						Log.e("insert", "Successfully sent to the replica 1:hash: " + filename + "port: " + firstSuccMap.get(port));
					}
					/* Key doesn;t belong to me but I am the second replica */
					if(secondSuccMap.get(port).compareTo(nodePort) == 0) {
						Log.e("insert", "Creating file " + filename);
						String string = values.get("value").toString();
						FileOutputStream outputStream;

						outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
						outputStream.write(string.getBytes());
						outputStream.close();
						insertedKeyList.add(values.get("key").toString());
						Log.e("insert", "Successfully inserted " + values.get("value").toString());
					} else {
						sendUpdate(secondSuccMap.get(port), msg);
						Log.e("insert", "Successfully sent to the replica 2:hash: " + filename + "port: " + secondSuccMap.get(port));
					}
				/*} else {*/
					//Log.e("insert", "Successfully sent to the right partition:hash: " + filename + "port: " + port);
				//}
			}
		} catch(Exception e) {
			Log.e("insert", "File write failed " + e.toString());
			e.printStackTrace();
		} finally {
			insertLock.unlock();
		}
		return uri;
	}

	/* This method initializes the predecessors and successors
	 */
	public void initNode() {
		if (nodePort.compareTo("11124") == 0) {
				node = portToHashes.get("11124");
				firstSuccNode = portToHashes.get("11112");
				firstSuccPort = "11112";
				secondSuccNode = portToHashes.get("11108");
				secondSuccPort = "11108";
				firstPredNode = portToHashes.get("11120");
				firstPredPort = "11120";
				secondPredNode = portToHashes.get("11116");
				secondPredPort = "11116";
		} else if (nodePort.compareTo("11112") == 0) {
				node = portToHashes.get("11112");
				firstSuccNode = portToHashes.get("11108");
				firstSuccPort = "11108";
				secondSuccNode = portToHashes.get("11116");
				secondSuccPort = "11116";
				firstPredNode = portToHashes.get("11124");
				firstPredPort = "11124";
				secondPredNode = portToHashes.get("11120");
				secondPredPort = "11120";
		} else if (nodePort.compareTo("11108") == 0) {
				node = portToHashes.get("11108");
				firstSuccNode = portToHashes.get("11116");
				firstSuccPort = "11116";
				secondSuccNode = portToHashes.get("11120");
				secondSuccPort = "11120";
				firstPredNode = portToHashes.get("11112");
				firstPredPort = "11112";
				secondPredNode = portToHashes.get("11124");
				secondPredPort = "11124";
		} else if (nodePort.compareTo("11116") == 0) {
				node = portToHashes.get("11116");
				firstSuccNode = portToHashes.get("11120");
				firstSuccPort = "11120";
				secondSuccNode = portToHashes.get("11124");
				secondSuccPort = "11124";
				firstPredNode = portToHashes.get("11108");
				firstPredPort = "11108";
				secondPredNode = portToHashes.get("11112");
				secondPredPort = "11112";
		} else if (nodePort.compareTo("11120") == 0) {
				node = portToHashes.get("11120");
				firstSuccNode = portToHashes.get("11124");
				firstSuccPort = "11124";
				secondSuccNode = portToHashes.get("11112");
				secondSuccPort = "11112";
				firstPredNode = portToHashes.get("11116");
				firstPredPort = "11116";
				secondPredNode = portToHashes.get("11108");
				secondPredPort = "11108";
		}
		Log.e("initNode", "node:" + node + "|firstPred:" + firstPredNode + ":|secondPred:" + secondPredNode
				+ "|firstSucc:" + firstSuccNode + "|secondSucc:" + secondSuccNode);
	}

	public void fetchFromPredcessors(String port) {
		Log.e("fetchFromPredcessors","Fetching everything from my predecessor:" + port);
		String receive = "";
		try {
			String remotePort = port;
			Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remotePort));
			//s.setSoTimeout(500);
			String msgToSend = "QUERY*" + "\n";

			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			Log.e("fetchFromPredecessors", "Sending message to predecessor");
			dos.writeBytes(msgToSend);
			dos.flush();

			// Receive response, all key values from cursor
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			receive = br.readLine();
			Log.e("fetchFromPredecessors", "Received :" + receive);
			if(receive == null || receive == "")
			{
				Log.e("fetchFromPredecessors", "Initially received null at startup, received:" + receive);
				return;
			}
			String [] keyvals = receive.split("-");
			for(int j = 0; j < keyvals.length; j+=2) {
				/* It may happen that when I am recovering, by second predecessor may be down
				* hence I will take the second predecessor related values first predecessor, which
				* keeps the replica of the second predecessor */
				if(checkPartition(genHash(keyvals[j])).compareTo(firstPredPort) == 0 ||
						checkPartition(genHash(keyvals[j])).compareTo(secondPredPort) == 0) {
					Log.e("fetchFromPredecessors", "Key:" + keyvals[j] + " belongs to my" +
							"predecessor:" + port);
					insertImm(mUri, keyvals[j], keyvals[j + 1]);
					//String msg = "REP" + "-" + keyvals[j] + "-" + keyvals[j+1];
					//sendUpdate(nodePort, msg);
				} else {
					Log.e("fetchFromPredecessors", "Key:" + keyvals[j] + " doesn't belongs to my" +
							"predecessor:" + port + " discard it");
				}
			}
		} catch (UnknownHostException e) {
			Log.e("fetchFromPredecessors", "UnknownHostException");
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("fetchFromPredecessors", "IOException");
			e.printStackTrace();
		} catch (Exception e) {
			Log.e("fetchFromPredecessors", "Generic Exception");
			Log.e("fetchFromPredecessors", "Initially received null at startup, received:" + receive);
			e.printStackTrace();
		}
		Log.e("fetchFromPredcessors","Exiting Fetching everything from my predecessor:");
		return;
	}

	public void fetchFromSuccessors(String port) {
		Log.e("fetchFromSuccessors","Fetching everything from my successor:" + port);
		String receive = "";
		try {
			String remotePort = port;
			Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remotePort));
			//s.setSoTimeout(2000);
			String msgToSend = "QUERY*" + "\n";

			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeBytes(msgToSend);
			dos.flush();

			// Receive response, all key values from cursor
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			receive = br.readLine();
			if(receive == null || receive == "")
			{
				Log.e("fetchFromSuccessors", "Initially received null at startup, received:" + receive);
				return;
			}
			String [] keyvals = receive.split("-");
			for(int j = 0; j < keyvals.length; j+=2) {
				if(checkPartition(genHash(keyvals[j])).compareTo(nodePort) == 0) {
					Log.e("fetchFromSuccessors", "Key:"+ keyvals[j] + " belongs to my" +
							"partition");
					//publishProgress();
					insertImm(mUri, keyvals[j], keyvals[j + 1]);
					//String msg = "REP" + "-" + keyvals[j] + "-" + keyvals[j+1];
					//sendUpdate(nodePort, msg);
				} else {
					Log.e("fetchFromSuccessors", "Key:"+ keyvals[j] + " doesn't belong to my" +
							"partition");
				}
			}
		} catch (UnknownHostException e) {
			Log.e("fetchFromSuccessors", "UnknownHostException");
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("fetchFromSuccessors", "IOException");
			e.printStackTrace();
		} catch (Exception e) {
			Log.e("fetchFromSuccessors", "Generic Exception");
			e.printStackTrace();
		}
		Log.e("fetchFromSuccessors","Exiting Fetching everything from my successors");
		return;
	}

	public void recoverNode() {
		/* Delete everything that I have */
		String [] files = getContext().fileList();
		for(int i = 0; i < files.length; i++)
			getContext().deleteFile(files[i]);

		//getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
		//delete(mUri, "@", null );
		/* Get everything from my first two predecessors */
		fetchFromPredcessors(firstPredPort);
		fetchFromPredcessors(secondPredPort);
		/* Get everything belonging to my partition from my first two successors */
		fetchFromSuccessors(firstSuccPort);
		fetchFromSuccessors(secondSuccPort);
		return;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() -4);
		final String myPort = String.valueOf(Integer.parseInt(portStr)*2);
		nodePort = myPort;

		populateHash();
		populateFirstSuccMap();
		populateSecondSuccMap();
		initNode();

		try {
			Log.e("onCreate", "Creating server task");
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch(IOException e) {
			Log.e("onCreate", "Can't create a ServerSocket!");
			e.printStackTrace();
			return false;
		}

		SharedPreferences sharedPref = this.getContext().getSharedPreferences("ff", Context.MODE_PRIVATE);
		if(sharedPref.getBoolean("recover", true)) {
			sharedPref.edit().putBoolean("recover", false).commit();
			Log.e("onCreate", "Non-failure case");
			/*SharedPreferences.Editor editor = sharedPref.edit();
			editor.putBoolean("recover", false);
			editor.commit();*/
		} else {
			Log.e("onCreate", "Creating client task for recovery");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
		}

		Log.e("onCreate", "Exiting onCreate");
		return true;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... strings) {
			try {
				Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodePort));
				//s.setSoTimeout(2000);
				String msgToSend = "RECOVER" + "\n";
				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeBytes(msgToSend);
				dos.flush();

            /*// Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor */
			} catch(SocketTimeoutException e) {
				Log.e("clientTask", " Socket Timeout Exception");
				e.printStackTrace();
			} catch(UnknownHostException e) {
				Log.e("clientTask", "UnknownHostException");
				e.printStackTrace();
			} catch(IOException e) {
				Log.e("clientTask", "IOException");
				e.printStackTrace();
			} catch(Exception e) {
				Log.e("clientTask", "Generic Exception");
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		ReentrantLock serverlock;
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			serverlock = new ReentrantLock();
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Log.e("ServerTask:doInBackgrd", "Server Running");
					Socket socket = serverSocket.accept();
					Log.e("ServerTask:doInBackgrd", "Server accepted connection");
					// Reader to read data from the client
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					// Stream to send data to the client
					PrintStream ps = new PrintStream(socket.getOutputStream());

					String receive = "";
					if ((receive = br.readLine()) != null) {
						Log.e("ServerTask:doInBackgrd", "Instruction received from the client " + receive);
						serverlock.lock();
						handleRequest(receive, ps);
						serverlock.unlock();
					} else {
						Log.e("ServerTask:doInBackgrd", "NULL data received from the client");
					}
				}
			} catch (IOException e) {
				Log.e("ServerTask:doInBackgrd", "Socket IO Exception");
				e.printStackTrace();
				serverlock.unlock();
			} catch (Exception e) {
				Log.e("ServerTask:doInBackgrd", "Generic Server Exception");
				e.printStackTrace();
				serverlock.unlock();
			}
			return null;
		}
	}

	public void handleRequest(String message, PrintStream ps) {
		Log.e("handleRequest", "Entering handleRequest");
		String [] msgtokens = message.split("-");

		Log.e("handleRequest", "Request from client " + msgtokens[0]);
		if(msgtokens[0].equalsIgnoreCase("INSERT")) {
			insertImm(mUri, msgtokens[1], msgtokens[2]);
			String msg = "REP" + "-" + msgtokens[1] + "-" + msgtokens[2];
			replicateInsert(msg);
		} else if(msgtokens[0].equalsIgnoreCase("RECPRED")) {
			String [] col = {"key", "value"};
			String keyval = "";
			MatrixCursor cur = (MatrixCursor)query(mUri, null, "@", null, null);
			cur.moveToFirst();
			if(cur.getCount() > 0) {
				try {
					do {
						String key = cur.getString(cur.getColumnIndex("key"));
						String val = cur.getString(cur.getColumnIndex("value"));
						if (checkPartition(genHash(key)).compareTo(nodePort) == 0) {
							keyval = key + "-" + val + "-";
						}
					} while (cur.moveToNext());
					cur.close();
					if (keyval.length() > 0) {
						keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
					}
				} catch(NoSuchAlgorithmException e) {
					Log.e("handleRequest", "Exception in RECPRED");
					e.printStackTrace();
				}
			}
			keyval = keyval + "\n";
			ps.println(keyval);
			Log.e("handleRequest", "Send all keyval for * query " + keyval);
		} else if(msgtokens[0].equalsIgnoreCase("RECOVER")) {
			Log.e("handleRequest", "Server received recover message");
			recoverNode();
		} else if(msgtokens[0].equalsIgnoreCase("REP")) {
			insertImm(mUri, msgtokens[1], msgtokens[2]);
		} else if(msgtokens[0].equalsIgnoreCase("DELETE")) {
			delete(mUri, msgtokens[1], null);
			//Delete from my replicas
			String msg = "DELREP" + "-" + msgtokens[1];
			deleteReplica(msg);
		} else if(msgtokens[0].equalsIgnoreCase("DELETE*")) {
			delete(mUri, "@", null );
		} else if(msgtokens[0].equalsIgnoreCase("DELREP")) {
			deleteImm(mUri, msgtokens[1]);
		} else if(msgtokens[0].equalsIgnoreCase("QUERY")) {
			try {
				String filename = genHash(msgtokens[1]);
				String [] col = {"key", "value"};
				String keyval = "";
				MatrixCursor cur = (MatrixCursor)query(mUri, null, msgtokens[1], null, null);
				cur.moveToFirst();
				if(cur.getCount() > 0) {
					keyval = cur.getString(cur.getColumnIndex("key")) + "-" +
							cur.getString(cur.getColumnIndex("value"));
				}
				cur.close();
				keyval = keyval + "\n";
				ps.println(keyval);
				Log.e("handleRequest", "Send keyval query " + keyval);
			} catch(NoSuchAlgorithmException e) {
				Log.e("handleRequest", "No such algo exception");
				e.printStackTrace();
			}

		} else if(msgtokens[0].equalsIgnoreCase("QUERY*")) {
			String [] col = {"key", "value"};
			String keyval = "";
			MatrixCursor cur = (MatrixCursor)query(mUri, null, "@", null, null);
			cur.moveToFirst();
			if(cur.getCount() > 0) {
				do {
					String key = cur.getString(cur.getColumnIndex("key"));
					String val = cur.getString(cur.getColumnIndex("value"));
					keyval = keyval + key + "-" + val + "-";
				} while(cur.moveToNext());
				cur.close();if(keyval.length() > 0) {
					keyval = keyval.substring(0, keyval.length()-1); // Removing the last extra "-"
				}
			}
			keyval = keyval + "\n";
			ps.println(keyval);
			Log.e("handleRequest", "Send all keyval for * query " + keyval);
		} else if(msgtokens[0].equalsIgnoreCase("QUERYREP")) {
			try {
				String filename = genHash(msgtokens[1]);
				String [] col = {"key", "value"};
				String keyval = "";
				MatrixCursor cur = (MatrixCursor)queryImm(mUri, null, msgtokens[1], null, null);
				cur.moveToFirst();
				if(cur.getCount() > 0) {
					keyval = cur.getString(cur.getColumnIndex("key")) + "-" +
							cur.getString(cur.getColumnIndex("value"));
				}
				cur.close();
				keyval = keyval + "\n";
				ps.println(keyval);
				Log.e("handleRequest", "Send keyval query rep" + keyval);
			} catch(NoSuchAlgorithmException e) {
				Log.e("handleRequest", "No such algo exception");
				e.printStackTrace();
			}
		}
		return;
	}

	public Cursor queryImm(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		Log.e("queryImm", selection);
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);
		try {
			String filename = genHash(selection);
			FileInputStream inputStream;
			inputStream = getContext().openFileInput(filename);
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String value = bufferedReader.readLine();
			Log.e("queryImm", "Value for Selection " + filename + " is " + value);
			String[] row = {selection, value};
			cursor.addRow(row);
		} catch(Exception e) {
			Log.e("queryImm", "Exception in queryingImm");
			e.printStackTrace();
		}
		return cursor;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		Log.e("query", selection);
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);
		//queryLock.lock();
		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e("query", "Query is @");
				for(int i = 0; i < insertedKeyList.size(); i++) {
					String filename = genHash(insertedKeyList.get(i));
					FileInputStream inputStream;

					inputStream = getContext().openFileInput(filename);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String value = bufferedReader.readLine();
					Log.e("query", "Value for Selection " + filename + " is " + value);

					String[] row = {insertedKeyList.get(i), value};

					cursor.addRow(row);
				}
		//		queryLock.unlock();
				if(selection.compareTo("*") == 0)
				{
					return starQuery(cursor);
				}
			} else {
				String filename = genHash(selection);
				String port = checkPartition(filename);
				if(nodePort.compareTo(port) == 0) {
					// Hash belongs to me
					FileInputStream inputStream;
					inputStream = getContext().openFileInput(filename);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String value = bufferedReader.readLine();
					Log.e("query", "Value for Selection " + filename + " is " + value);
					String[] row = {selection, value};
					cursor.addRow(row);
				} else { // Forward to the one who has and get the value from him
					String msg = "QUERY" + "-" + selection + "-" + nodePort;
					MatrixCursor cc = getQueryResponse(port, msg, cursor);
					if(cc.getCount() == 1) {
		//				queryLock.unlock();
						return cc;
					} else{
						Log.e("query", "The owner of the key " + port + " failed");
						Log.e("query", "Get the key from replicas");
						msg = "QUERYREP" + "-" + selection + "-" + nodePort;
		//				queryLock.unlock();
						return getQueryResponse(firstSuccMap.get(port), msg, cursor);
					}
				}
			}
		} catch(Exception e) {
			Log.e("query", "Exception in querying");
			e.printStackTrace();
		//	queryLock.unlock();
		}
		return cursor;
	}

	public MatrixCursor starQuery(MatrixCursor cur) {
		Log.e("starQuery","Entering starQuery");
		for(int i = 0; i < portList.length; i++) {
			try {
				String remotePort = portList[i];
				if(remotePort != nodePort) { // Not my port
					Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					String msgToSend = "QUERY*" + "\n";

					OutputStream out = s.getOutputStream();
					DataOutputStream dos = new DataOutputStream(out);
					dos.writeBytes(msgToSend);
					dos.flush();

					// Receive response, all key values from cursor
					BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String receive = br.readLine();
					String [] keyvals = receive.split("-");
					for(int j = 0; j < keyvals.length; j+=2) {
						String[] row = {keyvals[j], keyvals[j+1]};
						cur.addRow(row);
					}
				}
			} catch (UnknownHostException e) {
				Log.e("starQuery", "ClientSide UnknownHostException");
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("starQuery", "ClientSide IOException");
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("starQuery", "ClientSide Generic Exception");
				e.printStackTrace();
			}
		}
		return cur;
	}

	public MatrixCursor getQueryResponse(String port, String msg, MatrixCursor cur) {
		Log.e("getQueryResponse","Entering getQueryResponse(..)");
		try {
			Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			String msgToSend = msg + "\n";
			// Send query
			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeBytes(msgToSend);
			dos.flush();
			// Receive response
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String receive = br.readLine();
			//Populate cursor and return
			String [] keyvals = receive.split("-");
			String[] row = {keyvals[0], keyvals[1]};
			cur.addRow(row);
		} catch(UnknownHostException e) {
			Log.e("getQueryResponse", "getQueryResponse UnknownHostException");
			e.printStackTrace();
		} catch(IOException e) {
			Log.e("getQueryResponse", "getQueryResponse IOException");
			e.printStackTrace();
		} catch(Exception e) {
			Log.e("getQueryResponse", "getQueryResponse ClientSide Generic Exception");
			e.printStackTrace();
		}
		return cur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
