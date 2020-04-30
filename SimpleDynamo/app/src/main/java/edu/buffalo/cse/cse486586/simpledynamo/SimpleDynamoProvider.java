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
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
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
	ArrayList<String> insertedKeyList = new ArrayList<String>();
	String predNode = null; // Predecessor hash and port
	String predPort = null;
	String firstSuccNode = null; // First successor hash and port
	String firstSuccPort = null;
	String secondSuccNode = null; // Second successor hash and port
	String secondSuccPort = null;
	String node = ""; // My own hash and port
	String nodePort = "";

	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	protected void starDeleteQuery() {
		Log.e(TAG,"Entering starDeleteQuery");
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
				Log.e(TAG, "ClientSide UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientSide IOException");
			} catch (Exception e) {
				Log.e(TAG, "ClientSide Generic Exception");
			}
		}
		return;
	}

	protected void deleteImm(Uri uri, String key) {
		try {
			String filename = genHash(key);
			boolean res = getContext().deleteFile(filename);
			Log.e("delete", "Deleted successfully" + filename);
			insertedKeyList.remove(key);
		} catch(NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algorithm");
		}
		return;
	}

	protected void deleteReplica(String msg) {
		sendUpdate(firstSuccPort, msg);
		sendUpdate(secondSuccPort, msg);
		return;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v("delete", selection);
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);

		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e(TAG, "Delete selection is @");
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
				} else { // Forward to the node where the key belongs
					String msg = "DELETE" + "-" + selection;
					sendUpdate(port, msg);
				}
			}
		} catch(Exception e) {
			Log.e("delete", "Exception in deleting");
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	protected void populateHash() {
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
			Log.e(TAG, "No such algorithm exception");
		}
	}

	String checkPartition(String keyhash) {
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

	protected void sendUpdate(String port, String msg) {
		Log.e(TAG,"Send Update message " + msg + " to port " + port);
		try {
			Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			String msgToSend = msg + "\n";
			// Send Joining request
			OutputStream out = s.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeBytes(msgToSend);
			dos.flush();

            /*// Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor */
		} catch(UnknownHostException e) {
			Log.e("sendUpdate", "UnknownHostException");
		} catch(IOException e) {
			Log.e("sendUpdate", "IOException");
		} catch(Exception e) {
			Log.e("sendUpdate", "Generic Exception");
		}
		return;
	}

	protected void replicateInsert(String msg) {
		sendUpdate(firstSuccPort, msg);
		sendUpdate(secondSuccPort, msg);
	}

	void insertImm(Uri uri, String key, String value) {
		Log.e("insertImm", "Entering insert for key: " + key
				+ " value:" + value);
		try {
			String filename = genHash(key);
			Log.v("insertImm", "Creating file " + filename);
			String string = value;
			FileOutputStream outputStream;

				outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.close();
				insertedKeyList.add(key);
				Log.v("insertImm", "Successfully inserted " + value);
		} catch(Exception e) {
			Log.e("insert", "File write failed " + e.toString());
		}
		return;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.e("insert", "Entering insert for key: " + values.get("key").toString()
				+ " value:" + values.get("value").toString());
		try {
			String filename = genHash(values.get("key").toString());
			String port = checkPartition(filename);
			if(port.compareTo(nodePort) == 0) {
				Log.v("insert", "Creating file " + filename);
				String string = values.get("value").toString();
				FileOutputStream outputStream;

				outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.close();
				insertedKeyList.add(values.get("key").toString());
				Log.v("insert", "Successfully inserted " + values.get("value").toString());

				// Replicate to two successors
				String msg = "REP" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
				replicateInsert(msg);
			} else { // Sends it to the succNode
				String msg = "INSERT" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
				sendUpdate(port, msg);
			}
		} catch(Exception e) {
			Log.e("insert", "File write failed " + e.toString());
		}
		return uri;
	}

	/* This method initializes the predecessors and successors
	 */
	protected void initNode() {
		if (nodePort.compareTo("11124") == 0) {
				node = portToHashes.get("11124");
				firstSuccNode = portToHashes.get("11112");
				firstSuccPort = "11112";
				secondSuccNode = portToHashes.get("11108");
				secondSuccPort = "11108";
				predNode = portToHashes.get("11120");
				predPort = "11120";
		} else if (nodePort.compareTo("11112") == 0) {
				node = portToHashes.get("11112");
				firstSuccNode = portToHashes.get("11108");
				firstSuccPort = "11108";
				secondSuccNode = portToHashes.get("11116");
				secondSuccPort = "11116";
				predNode = portToHashes.get("11124");
				predPort = "11124";
		} else if (nodePort.compareTo("11108") == 0) {
				node = portToHashes.get("11108");
				firstSuccNode = portToHashes.get("11116");
				firstSuccPort = "11116";
				secondSuccNode = portToHashes.get("11120");
				secondSuccPort = "11120";
				predNode = portToHashes.get("11112");
				predPort = "11112";
		} else if (nodePort.compareTo("11116") == 0) {
				node = portToHashes.get("11116");
				firstSuccNode = portToHashes.get("11120");
				firstSuccPort = "11120";
				secondSuccNode = portToHashes.get("11124");
				secondSuccPort = "11124";
				predNode = portToHashes.get("11108");
				predPort = "11108";
		} else if (nodePort.compareTo("11120") == 0) {
				node = portToHashes.get("11120");
				firstSuccNode = portToHashes.get("11124");
				firstSuccPort = "11124";
				secondSuccNode = portToHashes.get("11112");
				secondSuccPort = "11112";
				predNode = portToHashes.get("11116");
				predPort = "11116";
		}
		Log.e(TAG, "node:" + node + "|pred:" + predNode + "|firstSucc:" +
				firstSuccNode + "|secondSucc:" + secondSuccNode);
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() -4);
		final String myPort = String.valueOf(Integer.parseInt(portStr)*2);
		nodePort = myPort;

		populateHash();
		initNode();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch(IOException e) {
			Log.e(TAG, "Can't create a ServerSocket!");
			return false;
		}
		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Log.e(TAG, "Server Running");
					Socket socket = serverSocket.accept();
					Log.e(TAG, "Server accepted connection");
					// Reader to read data from the client
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					// Stream to send data to the client
					PrintStream ps = new PrintStream(socket.getOutputStream());

					String receive = "";
					if ((receive = br.readLine()) != null) {
						Log.e(TAG, "Instruction received from the client " + receive);
						handleRequest(receive, ps);
					} else {
						Log.e(TAG, "NULL data received from the client");
					}
				}
			} catch (IOException e) {
				Log.e(TAG, "Socket IO Exception");
			} catch (Exception e) {
				Log.e(TAG, "Generic Server Exception");
			}
			return null;
		}
	}

	protected void handleRequest(String message, PrintStream ps) {
		Log.e("handleRequest", "Entering handleRequest");
		String [] msgtokens = message.split("-");

		Log.e("handleRequest", "Request from client " + msgtokens[0]);
		if(msgtokens[0].equalsIgnoreCase("INSERT")) {
			insertImm(mUri, msgtokens[1], msgtokens[2]);
			String msg = "REP" + "-" + msgtokens[1] + "-" + msgtokens[2];
			replicateInsert(msg);
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
				Log.e(TAG, "Send keyval query " + keyval);
			} catch(NoSuchAlgorithmException e) {
				Log.e(TAG, "No such algo exception");
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
			Log.e(TAG, "Send all keyval for * query " + keyval);
		}
		return;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.v("query", selection);
		Log.v("query", selection);
		String[] col = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(col);

		try {
			if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
				Log.e(TAG, "Query is @");
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
					return getQueryResponse(port, msg, cursor);
				}
			}
		} catch(Exception e) {
			Log.e("query", "Exception in querying");
		}
		return cursor;
	}
	protected MatrixCursor starQuery(MatrixCursor cur) {
		Log.e(TAG,"Entering starQuery");
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
				Log.e(TAG, "ClientSide UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientSide IOException");
			} catch (Exception e) {
				Log.e(TAG, "ClientSide Generic Exception");
			}
		}
		return cur;
	}

	protected MatrixCursor getQueryResponse(String port, String msg, MatrixCursor cur) {
		Log.e(TAG,"getQueryResponse");
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
			Log.e(TAG, "getQueryResponse UnknownHostException");
		} catch(IOException e) {
			Log.e(TAG, "getQueryResponse IOException");
		} catch(Exception e) {
			Log.e(TAG, "getQueryResponse ClientSide Generic Exception");
		}
		return cur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
