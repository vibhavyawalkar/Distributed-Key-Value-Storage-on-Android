package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
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
	HashMap<String, String> firstPredMap = new HashMap<String, String>();
	HashMap<String, String> secondPredMap = new HashMap<String, String>();
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
	static int recoverFlag = 0;
	ReentrantLock readLock = new ReentrantLock();
	ReentrantLock writeLock = new ReentrantLock();
	ReentrantLock recoveryLock = new ReentrantLock();

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
				if (remotePort.compareTo(nodePort) != 0) { // Not my port
					Socket s = new Socket();
					SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					s.connect(socketAddress,1000);
					String msgToSend = "DELETE*";

					OutputStream out = s.getOutputStream();
					DataOutputStream dos = new DataOutputStream(out);
					dos.writeUTF(msgToSend);
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

	public void deleteReplica(String msg) {
		sendUpdate(firstSuccPort, msg);
		sendUpdate(secondSuccPort, msg);
		return;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.e("delete", selection);
		while(recoverFlag == 1) {
			try {
				Log.e("delete", "Node is recovering, wait!!");
				Thread.sleep(100);
			} catch(Exception e) {
				Log.e("delete", "Exception in delete" + e);
				e.printStackTrace();
			}
		}
		writeLock.lock();
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);

		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e("delete", "Delete selection is @");
				for(int i = 0; i < insertedKeyList.size(); i++) {
					String filename = genHash(insertedKeyList.get(i));
					boolean res = getContext().deleteFile(filename);

					Log.e("delete", "Deleted successfully :" + filename);
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
						/* Delete contents from the replicas */
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
			Log.e("delete", "Finally we release the delete lock");
			writeLock.unlock();
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

	public void populateFirstPredMap() {
		firstPredMap.put("11108", "11112");
		firstPredMap.put("11116", "11108");
		firstPredMap.put("11120", "11116");
		firstPredMap.put("11124", "11120");
		firstPredMap.put("11112", "11124");
	}

	public void populateSecondPredMap() {
		secondPredMap.put("11108", "11124");
		secondPredMap.put("11116", "11112");
		secondPredMap.put("11120", "11108");
		secondPredMap.put("11124", "11116");
		secondPredMap.put("11112", "11120");
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

	public void sendUpdate(String port, String msg) {
		Log.e("sendUpdate","Send Update message " + msg + " to port " + port);
		try {
			Socket s = new Socket();
			SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			s.connect(socketAddress,1000);
			String msgToSend = msg;

			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeUTF(msgToSend);
			dos.flush();

            /*// Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor */
		} catch(Exception e) {
			Log.e("sendUpdate", "Generic Exception"+ e.toString());
			e.printStackTrace();
		}
		return;
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
		populateFirstPredMap();
		populateSecondPredMap();
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
			try {
				recoverFlag = 1;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
				Log.e("onCreate", "Returned from recovery task");
			} catch(Exception e) {
				Log.e("onCreate", "Exception in recovery" + e);
			}

		}

		Log.e("onCreate", "Exiting onCreate");
		return true;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... strings) {
			Log.e("recoveryTask", "Entering recovery task");
			try {
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodePort));
				s.connect(socketAddress,2000);
				String msgToSend = "RECOVER";
				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

            /*// Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor */
			} catch(SocketTimeoutException e) {
				Log.e("clientTask", " Socket Timeout Exception" + e.toString());
				e.printStackTrace();
			} catch(UnknownHostException e) {
				Log.e("clientTask", "UnknownHostException" + e.toString());
				e.printStackTrace();
			} catch(IOException e) {
				Log.e("clientTask", "IOException" + e.toString());
				e.printStackTrace();
			} catch(Exception e) {
				Log.e("clientTask", "Generic Exception" + e.toString());
				e.printStackTrace();
			}
			Log.e("recoveryTask", "Exiting recovery task");
			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Log.e("ServerTask:doInBackgrd", "Server Running");
					Socket socket = serverSocket.accept();
					Log.e("ServerTask:doInBackgrd", "Server accepted connection");
					DataInputStream ds = new DataInputStream(socket.getInputStream());

					/*
					// Reader to read data from the client
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					// Stream to send data to the client
					PrintStream prnts = new PrintStream(socket.getOutputStream());
					*/
					DataOutputStream ps = new DataOutputStream(socket.getOutputStream());
					String receive = "";
					if ((receive = ds.readUTF()) != null || receive != "")  {
						Log.e("ServerTask:doInBackgrd", "Instruction received from the client " + receive);
						handleRequest(receive, ps);
					} else {
						Log.e("ServerTask:doInBackgrd", "NULL data received from the client");
					}
				}
			} catch (IOException e) {
				Log.e("ServerTask:doInBackgrd", "Socket IO Exception" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("ServerTask:doInBackgrd", "Generic Server Exception" + e.toString());
				e.printStackTrace();
			}
			return null;
		}

		public void handleRequest(String message, DataOutputStream ps) {
			Log.e("handleRequest", "Entering handleRequest");
			String[] msgtokens = message.split("-");

			Log.e("handleRequest", "Request from client " + msgtokens[0] + "|");
			if (msgtokens[0].equalsIgnoreCase("INSERT")) {
				insertImm(mUri, msgtokens[1], msgtokens[2]);
			} else if (msgtokens[0].equalsIgnoreCase("RECOVER")) {
				Log.e("handleRequest", "Server received recover message");
				recoverNode();
				recoverFlag = 0;
			} else if (msgtokens[0].equalsIgnoreCase("REP")) {
				insertImm(mUri, msgtokens[1], msgtokens[2]);
			} else if (msgtokens[0].equalsIgnoreCase("DELETE*")) {
				delete(mUri, "@", null);
			} else if (msgtokens[0].equalsIgnoreCase("DELREP")) {
				deleteImm(mUri, msgtokens[1]);
			} else if (msgtokens[0].equalsIgnoreCase("QUERY")) {
				try {
					String filename = genHash(msgtokens[1]);
					String[] col = {"key", "value"};
					String keyval = "";
					MatrixCursor cur = (MatrixCursor) queryImm(mUri, null, msgtokens[1], null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						keyval = cur.getString(cur.getColumnIndex("key")) + "-" +
								cur.getString(cur.getColumnIndex("value"));
					}
					cur.close();
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send keyval query " + keyval);
				} catch (Exception e) {
					Log.e("handleRequest", "Exception found in QUERY:" + e);
					e.printStackTrace();
				}
			} else if(msgtokens[0].equalsIgnoreCase("STARQUERY")) {
				try {
					String[] col = {"key", "value"};

					String keyval = "";
					MatrixCursor cur = (MatrixCursor) query(mUri, null, "@", null, null);
					int count = 0;
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						do {
							String key = cur.getString(cur.getColumnIndex("key"));
							String val = cur.getString(cur.getColumnIndex("value"));
							keyval = keyval + key + "-" + val + "-";
							count++;
							if(count > 800) {
								count = 0;
								if (keyval.length() > 0) {
									keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
								}
								ps.writeUTF(keyval);
								ps.flush();
								keyval = "";
							}
						} while (cur.moveToNext());
						cur.close();
						if (keyval.length() > 0) {
							keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
						}
					}
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send all keyval for * query " + keyval);
					Log.e("handleRequest", "Exiting STARQUERY");
				} catch (Exception e) {
					Log.e("handleRequest", "Exception caught in STARQUERY:" + e);
				}
			} else if (msgtokens[0].equalsIgnoreCase("QUERY*")) {
				try {
					String[] col = {"key", "value"};

					String keyval = "";
					MatrixCursor cur = (MatrixCursor) query(mUri, null, "@", null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						do {
							String key = cur.getString(cur.getColumnIndex("key"));
							String val = cur.getString(cur.getColumnIndex("value"));
							keyval = keyval + key + "-" + val + "-";
						} while (cur.moveToNext());
						cur.close();
						if (keyval.length() > 0) {
							keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
						}
					}
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send all keyval for * query " + keyval);
				} catch(Exception e) {
					Log.e("handleRequest", "Exception caught in QUERY*:" + e);
				}
			} else if(msgtokens[0].equalsIgnoreCase("SECONDSUCC")) {
				try {
					String[] col = {"key", "value"};

					String keyval = "";
					MatrixCursor cur = (MatrixCursor) query(mUri, null, "@", null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						do {
							String key = cur.getString(cur.getColumnIndex("key"));
							String val = cur.getString(cur.getColumnIndex("value"));
							/* msgtokens[1] contains the port of the node which is recovering */
							if(checkPartition(genHash(key)).compareTo(msgtokens[1]) == 0)
								keyval = keyval + key + "-" + val + "-";
						} while (cur.moveToNext());
						cur.close();
						if (keyval.length() > 0) {
							keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
						}
					}
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send all keyval for SECONDSUCC " + keyval);
				} catch(Exception e) {
					Log.e("handleRequest", "Exception caught in SECONDSUCC" + e);
				}
			} else if(msgtokens[0].equalsIgnoreCase("FIRSTSUCC")) {
				Log.e("handleRequest", "Received FIRSTSUCC from " + msgtokens[1]);
				try {
					String[] col = {"key", "value"};

					String keyval = "";
					MatrixCursor cur = (MatrixCursor) query(mUri, null, "@", null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						do {
							String key = cur.getString(cur.getColumnIndex("key"));
							String val = cur.getString(cur.getColumnIndex("value"));
							/* msgtokens[1] contains the port of the node which is recovering */
							if(checkPartition(genHash(key)).compareTo(firstPredMap.get(msgtokens[1])) == 0 ||
							checkPartition(genHash(key)).compareTo(msgtokens[1]) == 0)
								keyval = keyval + key + "-" + val + "-";
						} while (cur.moveToNext());
						cur.close();
						if (keyval.length() > 0) {
							keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
						}
					}
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send all keyval for FIRSTSUCC " + keyval);
				} catch(Exception e) {
					Log.e("handleRequest", "Exception caught in FIRSTSUCC:" + e);
				}
			} else if(msgtokens[0].equalsIgnoreCase("FIRSTPRED")) {
				try {
					String[] col = {"key", "value"};

					String keyval = "";
					MatrixCursor cur = (MatrixCursor) query(mUri, null, "@", null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						do {
							String key = cur.getString(cur.getColumnIndex("key"));
							String val = cur.getString(cur.getColumnIndex("value"));
							/* msgtokens[1] contains the port of the node which is recovering */
							if(checkPartition(genHash(key)).compareTo(secondPredMap.get(msgtokens[1])) == 0
							|| checkPartition(genHash(key)).compareTo(nodePort) == 0 )
								keyval = keyval + key + "-" + val + "-";
						} while (cur.moveToNext());
						cur.close();
						if (keyval.length() > 0) {
							keyval = keyval.substring(0, keyval.length() - 1); // Removing the last extra "-"
						}
					}
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send all keyval for FIRSTSUCC " + keyval);
				} catch(Exception e) {
					Log.e("handleRequest", "Exception caught in FIRSTSUCC:" + e);
				}
			}
			else if (msgtokens[0].equalsIgnoreCase("QUERYREP")){
				try {
					String filename = genHash(msgtokens[1]);
					String[] col = {"key", "value"};
					String keyval = "";
					MatrixCursor cur = (MatrixCursor) queryImm(mUri, null, msgtokens[1], null, null);
					cur.moveToFirst();
					if (cur.getCount() > 0) {
						keyval = cur.getString(cur.getColumnIndex("key")) + "-" +
								cur.getString(cur.getColumnIndex("value"));
					}
					cur.close();
					ps.writeUTF(keyval);
					ps.flush();
					Log.e("handleRequest", "Send keyval query rep" + keyval);
				} catch (Exception e) {
					Log.e("handleRequest", "Exception in QUERYREP" + e);
					e.printStackTrace();
				}
			}
			return;
		}

		public void fetchmyReplicaTwo(String port) {
			Log.e("fetchmyReplicaTwo","Fetching everything from my firstPredecessor:" + port);
			String receive = "";
			try {
				String remotePort = port;
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				s.connect(socketAddress,1000);
				String msgToSend = "QUERY*";

				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

				// Receive response, all key values from cursor
				DataInputStream ds = new DataInputStream(s.getInputStream());
				receive = ds.readUTF();
				/*
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				receive = br.readLine();
				*/
				if(receive == null || receive == "")
				{
					Log.e("fetchmyReplicaTwo", "Initially received null at startup, received:" + receive);
					return;
				}
				String [] keyvals = receive.split("-");
				Log.e("fetchmyReplicaTwo", "Length of keyval array:" + keyvals.length);
				for(int j = 0; j < keyvals.length; j = j + 2) {
					if(checkPartition(genHash(keyvals[j])).compareTo(secondPredPort) == 0 ||
							checkPartition(genHash(keyvals[j])).compareTo(firstPredPort) == 0 ) {
						Log.e("fetchmyReplicaTwo", "Key:"+ keyvals[j] + " belongs to my second Prednode" + secondPredPort);
						insertImm(mUri, keyvals[j], keyvals[j + 1]);
					} else {
						Log.e("fetchmyReplicaTwo", "Key:"+ keyvals[j] + " doesn't belong to my second PredNode" + secondPredPort);
					}
				}
			} catch (UnknownHostException e) {
				Log.e("fetchmyReplicaTwo", "UnknownHostException" + e);
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("fetchmyReplicaTwo", "IOException" + e);
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("fetchmyReplicaTwo", "Generic Exception" + e);
				e.printStackTrace();
			}
			Log.e("fetchFromReplicaTwo","Exiting Fetching everything from my firstPredecessor:");
			return;
		}


		public void fetchmyReplicaOne(String port) {
			Log.e("fetchmyReplicaOne","Fetching everything from my firstSuccessor:" + port);
			String receive = "";
			try {
				String remotePort = port;
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				s.connect(socketAddress,1000);
				String msgToSend = "QUERY*";

				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

				// Receive response, all key values from cursor
				DataInputStream ds = new DataInputStream(s.getInputStream());
				receive = ds.readUTF();
				/*
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				receive = br.readLine();
				*/
				if(receive == null || receive == "")
				{
					Log.e("fetchmyReplicaOne", "Initially received null at startup, received:" + receive);
					return;
				}
				String [] keyvals = receive.split("-");
				for(int j = 0; j < keyvals.length; j = j + 2) {
					if(checkPartition(genHash(keyvals[j])).compareTo(firstPredPort) == 0) {
						Log.e("fetchmyReplicaOne", "Key:"+ keyvals[j] + " belongs to my first Prednode" + firstPredPort);
						insertImm(mUri, keyvals[j], keyvals[j + 1]);
					} else {
						Log.e("fetchmyReplicaOne", "Key:"+ keyvals[j] + " doesn't belong to my first PredNode" + firstPredPort);
					}
				}
			} catch (UnknownHostException e) {
				Log.e("fetchmyReplicaOne", "UnknownHostException" + e.toString());
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("fetchmyReplicaOne", "IOException" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("fetchmyReplicaOne", "Generic Exception" + e.toString());
				e.printStackTrace();
			}
			Log.e("fetchFromReplicaOne","Exiting Fetching everything from my firstSuccessor:");
			return;
		}

		public void fetchmyData(String port) {
			Log.e("fetchmyData","Fetching everything from my secondSuccessor:" + port);
			String receive = "";
			try {
				String remotePort = port;
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				s.connect(socketAddress,1000);
				String msgToSend = "STARQUERY";

				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

				// Receive response, all key values from cursor
				DataInputStream ds = new DataInputStream(s.getInputStream());
				receive = ds.readUTF();
				/*
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				receive = br.readLine();
				*/
				if(receive == null || receive == "")
				{
					Log.e("fetchmyData", "Initially received null at startup, received:" + receive);
					return;
				}
				boolean moreData = false;
				String [] keyvals = receive.split("-");
				if(keyvals.length == 0 || keyvals.length == 1)
					return;
				Log.e("fetchmyData", "Keyvals length:" + keyvals.length);
				if(keyvals.length >= 1600) {
					moreData = true;
				}
				for(int j = 0; j < keyvals.length; j=j+2) {
					if(checkPartition(genHash(keyvals[j])).compareTo(nodePort) == 0) {
						Log.e("fetchmyData", "Key:"+ keyvals[j] + " belongs to my" +
								"partition");
							insertImm(mUri, keyvals[j], keyvals[j + 1]);
						} else {
							Log.e("fetchmyData", "Key:"+ keyvals[j] + " doesn't belong to my" +
								"partition");
						}
				}
				if(moreData == true) {
					receive = ds.readUTF();
					String[] keyvals_ = receive.split("-");
					if(keyvals_.length == 0 || keyvals_.length == 1)
						return;
					Log.e("fetchmyData", "Keyvals extra length:" + keyvals_.length);
					for (int j = 0; j < keyvals_.length; j += 2) {
						if(checkPartition(genHash(keyvals[j])).compareTo(nodePort) == 0) {
							Log.e("fetchmyData", "Key:"+ keyvals[j] + " belongs to my" +
									"partition");
							insertImm(mUri, keyvals[j], keyvals[j + 1]);
						} else {
							Log.e("fetchmyData", "Key:"+ keyvals[j] + " doesn't belong to my" +
									"partition");
						}
					}
				}

			} catch (UnknownHostException e) {
				Log.e("fetchmyData", "UnknownHostException:" + e.toString());
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("fetchmyData", "IOException:" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("fetchmyData", "Generic Exception:" + e.toString());
				e.printStackTrace();
			}
			Log.e("fetchmyData","Exiting Fetching everything from my secondSuccessor:");
			return;
		}

		public void fetchpredData(String port) {
			Log.e("fetchpredData","Fetching everything from my secondSuccessor:" + port);
			String receive = "";
			try {
				String remotePort = port;
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				s.connect(socketAddress,1000);
				String msgToSend = "STARQUERY";

				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

				// Receive response, all key values from cursor
				DataInputStream ds = new DataInputStream(s.getInputStream());
				receive = ds.readUTF();
				/*
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				receive = br.readLine();
				*/
				if(receive == null || receive == "")
				{
					Log.e("fetchsecondpredData", "Initially received null at startup, received:" + receive);
					return;
				}
				boolean moreData = false;
				String [] keyvals = receive.split("-");
				if(keyvals.length == 0 || keyvals.length == 1)
					return;
				Log.e("fetchsecondpredData", "Keyvals length:" + keyvals.length);
				if(keyvals.length >= 1600) {
					moreData = true;
				}

				for(int j = 0; j < keyvals.length; j=j+2) {

					if(checkPartition(genHash(keyvals[j])).compareTo(port) == 0 ||
							checkPartition(genHash(keyvals[j])).compareTo(firstPredMap.get(port)) == 0) {
						Log.e("fetchsecondpredData", "Key:"+ keyvals[j] + " belongs to my" +
								"partition");
						insertImm(mUri, keyvals[j], keyvals[j + 1]);
					} else {
						Log.e("fetchsecondpredData", "Key:"+ keyvals[j] + " doesn't belong to my" +
								"partition");
					}
				}
				if(moreData == true) {
					receive = ds.readUTF();
					String[] keyvals_ = receive.split("-");
					if(keyvals_.length == 0 || keyvals_.length == 1)
						return;
					Log.e("fetchsecondpredData", "Keyvals extra length:" + keyvals_.length);
					for (int j = 0; j < keyvals_.length; j += 2) {
						if(checkPartition(genHash(keyvals[j])).compareTo(port) == 0 ) {
							Log.e("fetchsecondpredData", "Key:"+ keyvals[j] + " belongs to my" +
									"partition");
							insertImm(mUri, keyvals[j], keyvals[j + 1]);
						} else {
							Log.e("fetchsecondpredData", "Key:"+ keyvals[j] + " doesn't belong to my" +
									"partition");
						}
					}
				}

			} catch (UnknownHostException e) {
				Log.e("fetchsecondpredData", "UnknownHostException:" + e.toString());
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("fetchsecondpredData", "IOException:" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("fetchsecondpredData", "Generic Exception:" + e.toString());
				e.printStackTrace();
			}
			Log.e("fetchsecondpredData","Exiting Fetching everything from my secondpred:");
			return;
		}

		public void recoverData(String port, String cmd) {
			Log.e("recoverData","Recovering data from port:" + port);
			String receive = "";
			try {
				String remotePort = port;
				Socket s = new Socket();
				SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				s.connect(socketAddress,3000);
				String msgToSend = cmd + "-" + nodePort;

				OutputStream out = s.getOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				dos.writeUTF(msgToSend);
				dos.flush();

				// Receive response, all key values from cursor
				DataInputStream ds = new DataInputStream(s.getInputStream());
				receive = ds.readUTF();
				/*
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				receive = br.readLine();
				*/
				if(receive == null || receive == "")
				{
					Log.e("recoverData", "Received null in recover data" + receive);
					return;
				}
				String [] keyvals = receive.split("-");
				if(keyvals.length == 1 || keyvals.length == 0)
				{
					Log.e("recoverData", "No keys for recovery from port:" + port);
					return;
				}
				for(int j = 0; j < keyvals.length; j=j+2) {
					/*if(checkPartition(genHash(keyvals[j])).compareTo(nodePort) == 0) {
						Log.e("fetchmyData", "Key:"+ keyvals[j] + " belongs to my" +
								"partition");
					*/	insertImm(mUri, keyvals[j], keyvals[j + 1]);
					/*} else {
						Log.e("fetchmyData", "Key:"+ keyvals[j] + " doesn't belong to my" +
								"partition");
					}*/
				}
			} catch (UnknownHostException e) {
				Log.e("recoverData", "UnknownHostException:" + e.toString());
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("recoverData", "IOException:" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("recoverData", "Generic Exception:" + e.toString());
				e.printStackTrace();
			}
			Log.e("recoverData","Exiting recovering data form port:" + port);
			return;
		}

		public void recoverNode() {
			/* Delete everything that I have */
			String [] files = getContext().fileList();
			Log.e("recoverNode", "Number of files before recovery:"+ files.length);
			for(int i = 0; i < files.length; i++)
				getContext().deleteFile(files[i]);
			insertedKeyList.clear();
			//getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
			//delete(mUri, "@", null );


			recoverData(firstPredPort, "FIRSTPRED");
			recoverData(secondSuccPort, "SECONDSUCC");
			recoverData(firstSuccPort, "FIRSTSUCC");


			/*
			fetchmyData(firstSuccPort); // This doing as extra caution for asynchrony
			fetchmyData(secondSuccPort); // This is required => chain replication
			fetchmyReplicaOne(firstSuccPort); //This is required, => chain replication
			fetchmyReplicaTwo(firstPredPort);
			 */
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
			} catch (Exception e) {
				Log.e("queryImm", "Exception in queryingImm");
				e.printStackTrace();
			}
			return cursor;
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
 	} /* End of Async Task ServerTask */

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.e("insert", "Entering insert for key: " + values.get("key").toString()
				+ " value:" + values.get("value").toString());
		while(recoverFlag == 1) {
			try {
				Log.e("insert", "Node is recovering, wait!!");
				Thread.sleep(100);
			} catch(Exception e) {
				Log.e("insert", "Exception in insert" + e);
				e.printStackTrace();
			}
		}

		writeLock.lock();
		try {
			String filename = genHash(values.get("key").toString());
			String port = checkPartition(filename);
			if(port.compareTo(nodePort) == 0) {
				Log.e("insert", "Creating file " + filename);
				String string = values.get("value").toString();
				FileOutputStream outputStream;
/*
				if(true == insertedKeyList.contains(values.get("key").toString())) {
					Log.e("insert", "Deleting duplicate key :" + values.get("key").toString() + " hash:" +
							genHash(values.get("key").toString()));
					getContext().deleteFile(genHash(values.get("key").toString()));
				}*/
				outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.close();
				insertedKeyList.add(values.get("key").toString());
				Log.e("insert", "Successfully inserted " + values.get("value").toString());

				// Replicate to two successors
				String msg = "REP" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
				sendUpdate(firstSuccPort, msg);
				sendUpdate(secondSuccPort, msg);
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
			}
		} catch(Exception e) {
			Log.e("insert", "File write failed " + e.toString());
			e.printStackTrace();
		} finally {
			Log.e("insert", "Finally we release the insertLock");
			writeLock.unlock();
		}
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
							String[] selectionArgs, String sortOrder) {
		Log.e("query", selection);

		/* Keep waiting till recovery is completed */
		while(recoverFlag == 1) {
			try {
				Log.e("query", "Node is recovering, wait!!");
				Thread.sleep(100);
			} catch(Exception e) {
				Log.e("query", "Exception in query" + e);
				e.printStackTrace();
			}
		}
		readLock.lock();
		writeLock.lock();
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);
		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e("query", "Query is @");
				for (int i = 0; i < insertedKeyList.size(); i++) {
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
				if (selection.compareTo("*") == 0) {
					MatrixCursor cur = starQuery(cursor);
					return cur;
				}
			} else {
				String filename = genHash(selection);
				String port = checkPartition(filename);
				if (nodePort.compareTo(port) == 0) {
					// Hash belongs to me
					FileInputStream inputStream;
					inputStream = getContext().openFileInput(filename);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String value = bufferedReader.readLine();
					Log.e("query", "Value for Selection " + filename + " is " + value);
					String[] row = {selection, value};
					cursor.addRow(row);
				} else {
					String msg = "QUERYREP" + "-" + selection + "-" + nodePort;
					Log.e("query", "Get the key from replica 2"); // Chain replication
					MatrixCursor cc = getQueryResponse(secondSuccMap.get(port), msg, cursor);
					if(cc.getCount() == 1) {
						return cc;
					} else {
						Log.e("query", "Get the key from replica 1"); // Chain replication
						msg = "QUERYREP" + "-" + selection + "-" + nodePort;
						cc = getQueryResponse(firstSuccMap.get(port), msg, cursor);
					}
					if(cc.getCount() == 1) {
						msg = "QUERY" + "-" + selection + "-" + nodePort;
						return getQueryResponse(port, msg, cursor);
					}
				}
			}
		} catch (Exception e) {
			Log.e("query", "Exception in querying");
			e.printStackTrace();
		} finally {
			Log.e("query", "Finally we release the query lock");
			writeLock.unlock();
			readLock.unlock();
		}
		return cursor;
	}

	public MatrixCursor starQuery(MatrixCursor cur) {
		Log.e("starQuery", "Entering starQuery");
		for (int i = 0; i < portList.length; i++) {
			try {
				String remotePort = portList[i];
				if (remotePort.compareTo(nodePort) != 0) { // Not my port
					Socket s = new Socket();
					SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					s.connect(socketAddress, 2000);
					String msgToSend = "STARQUERY";
					Log.e("starQuery", "Sending message STARQUERY to port:" + remotePort);
					OutputStream out = s.getOutputStream();
					DataOutputStream dos = new DataOutputStream(out);
					dos.writeUTF(msgToSend);
					dos.flush();

					DataInputStream ds = new DataInputStream(s.getInputStream());
					String receive = ds.readUTF();
					boolean moreData = false;
					// Receive response, all key values from cursor
/*					BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String receive = br.readLine();*/
					String[] keyvals = receive.split("-");
					if(keyvals.length == 0 || keyvals.length == 1)
						return cur;
					Log.e("starQuery", "Keyvals length:" + keyvals.length);
					if(keyvals.length >= 1600) {
						moreData = true;
					}
					for (int j = 0; j < keyvals.length; j += 2) {
						String[] row = {keyvals[j], keyvals[j + 1]};
						cur.addRow(row);
					}
					if(moreData == true) {
						receive = ds.readUTF();
						String[] keyvals_ = receive.split("-");
						if(keyvals_.length == 0 || keyvals_.length == 1)
							return cur;
						Log.e("starQuery", "Keyvals extra length:" + keyvals_.length);
						for (int j = 0; j < keyvals_.length; j += 2) {
							String[] row = {keyvals_[j], keyvals_[j + 1]};
							cur.addRow(row);
						}
					}
				}
			} catch (UnknownHostException e) {
				Log.e("starQuery", "ClientSide UnknownHostException"+ e.toString());
				e.printStackTrace();
			} catch (IOException e) {
				Log.e("starQuery", "ClientSide IOException" + e.toString());
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("starQuery", "ClientSide Generic Exception"+e.toString());
				e.printStackTrace();
			}
		}
		Log.e("starQuery", "Exiting starQuery (..)");
		return cur;
	}

	public MatrixCursor getQueryResponse(String port, String msg, MatrixCursor cur) {
		Log.e("getQueryResponse", "Entering getQueryResponse(..)");
		Log.e("getQueryResponse", "Querying port:" + port);
		try {
			Socket s = new Socket();
			SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			s.connect(socketAddress, 2000);

			String msgToSend = msg;
			// Send query
			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeUTF(msgToSend);
			dos.flush();
			// Receive response

			DataInputStream ds = new DataInputStream(s.getInputStream());
			String receive = ds.readUTF();
			/*
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String receive = br.readLine();
			*/

			//Populate cursor and return
			String[] keyvals = receive.split("-");
			String[] row = {keyvals[0], keyvals[1]};
			cur.addRow(row);
		} catch (UnknownHostException e) {
			Log.e("getQueryResponse", "getQueryResponse UnknownHostException");
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("getQueryResponse", "getQueryResponse IOException");
			e.printStackTrace();
		} catch (Exception e) {
			Log.e("getQueryResponse", "getQueryResponse ClientSide Generic Exception");
			e.printStackTrace();
		}
		Log.e("getQueryResponse", "Exiting getQueryResponse(...)");
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
