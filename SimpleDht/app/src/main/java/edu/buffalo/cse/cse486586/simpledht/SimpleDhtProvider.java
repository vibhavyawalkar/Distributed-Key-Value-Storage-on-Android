package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataOutputStream;
import java.io.IOException;
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

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.opengl.Matrix;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.*;
import java.util.HashMap;

class nodeExists {
    String portStr;
    int joined;

    public nodeExists(String port, int joinStatus) {
        this.portStr = port;
        this.joined = joinStatus;
    }
}

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String [] portList = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    ArrayList<nodeExists> n = new ArrayList<nodeExists>();
    HashMap<String, String> portToHashes = new HashMap<String, String>();
    HashMap<String, String> hashToPort = new HashMap<String, String>();
    ArrayList<String> insertedKeyList = new ArrayList<String>();
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    String predNode = null;
    String predPort = null;
    String succNode = null;
    String succPort = null;
    String node = "";
    String nodePort = "";

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
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
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such algorithm exception");
        }
    }

    protected void populateArrayList() {
        n.add(new nodeExists("11124", 0));
        n.add(new nodeExists("11112", 0));
        n.add(new nodeExists("11108", 0));
        n.add(new nodeExists("11116", 0));
        n.add(new nodeExists("11120", 0));
    }

    protected void setJoined(String port) {
        for(int i = 0; i < n.size(); i++) {
            if(port.compareTo(n.get(i).portStr) == 0) {
                n.get(i).joined = 1;
            }
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        populateHash();
        populateArrayList();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() -4);
        final String myPort = String.valueOf(Integer.parseInt(portStr)*2);
        nodePort = myPort;
        setJoined(myPort);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(IOException e) {
            Log.e(TAG, "Can't create a ServerSocket!");
            return false;
        }
        try {
            node = genHash(portStr);
            Log.e(TAG, "My Hash:" + node);
        } catch(NoSuchAlgorithmException e) {
            Log.e(TAG, "NO such algorithm exception");
        }

        Log.e(TAG, "Preparing to send join request");
        if(!myPort.equals("11108")) {
            String msg = "JOIN" + "-" + myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        } else {
            Log.e(TAG, "Didn't send the join request: ");
        }

        return true;
    }

    protected void sendJoinRequest(String msg) {
        try {
            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
            String msgToSend = msg + "\n";
            // Send Joining request
            Log.e("sendJoinRequest", "Send joining request");
            OutputStream out = s.getOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeBytes(msgToSend);
            dos.flush();

            Log.e("sendJoinRequest", "Receive response for joining request");
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();

            String [] msgtokens = receive.split("-");

            if(msgtokens[0].equalsIgnoreCase("PREDSUCC")) {
                // Update the successor and predecessor
                predNode = portToHashes.get(msgtokens[1]);
                succNode = portToHashes.get(msgtokens[2]);
                predPort = msgtokens[1];
                succPort = msgtokens[2];
                Log.e(TAG, "PredHash:" + predNode + " " + "SuccHash:" + succNode);
            } else {
                Log.e(TAG,"Invalid response to the joining request");
            }
        } catch(UnknownHostException e) {
            Log.e("sendJoinRequest", "UnknownHostException");
        } catch(IOException e) {
            Log.e("sendJoinRequest", "IOException");
        } catch(Exception e) {
            Log.e("sendJoinRequest", "Generic Exception" + e);
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            sendJoinRequest(msgs[0].trim());
            return null;
        }
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
        if(msgtokens[0].equalsIgnoreCase("JOIN")){
            try {
                String nodePort = msgtokens[1];
                addToRingNew(nodePort, ps);
            } catch(Exception e) {
                Log.e("handleRequest", "Caught execption on the server, add to Ring");
            }
        } else if(msgtokens[0].equalsIgnoreCase("PREDSUCC")) {
            predNode = portToHashes.get(msgtokens[1]);
            succNode = portToHashes.get(msgtokens[2]);
            predPort = msgtokens[1];
            succPort = msgtokens[2];
            Log.e("handleRequest", "PredHash:" + predNode +" " + "SuccHash:" + succNode);
        } else if(msgtokens[0].equalsIgnoreCase("PRED")) {
            predNode = portToHashes.get(msgtokens[1]);
            predPort = msgtokens[1];
            Log.e("handleRequest", "PredHash:" + predNode +" " + "SuccHash:" + succNode);
        } else if(msgtokens[0].equalsIgnoreCase("SUCC")) {
            succNode = portToHashes.get(msgtokens[1]);
            succPort = msgtokens[1];
            Log.e("handleRequest", "PredHash:" + predNode +" " + "SuccHash:" + succNode);
        } else if(msgtokens[0].equalsIgnoreCase("INSERT")) {
            ContentValues values = new ContentValues();
            values.put("key", msgtokens[1]);
            values.put("value",msgtokens[2]);
            insert(mUri, values);
        } else if(msgtokens[0].equalsIgnoreCase("DELETE")) {
            delete(mUri, msgtokens[1], null);
        } else if(msgtokens[0].equalsIgnoreCase("DELETE*")) {
            delete(mUri, "@", null );
        } else if(msgtokens[0].equalsIgnoreCase("QUERY")) {
            try {
                String filename = genHash(msgtokens[1]);
                if(
                        (node.compareTo(filename) >= 0 && predNode.compareTo(filename) < 0) ||
                        (node.compareTo(predNode) < 0 && predNode.compareTo(filename) <= 0 && node.compareTo(filename) <=0) ||
                        (node.compareTo(predNode) < 0 && node.compareTo(filename) >= 0 && predNode.compareTo(filename) >= 0))
                {
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
                    //sendUpdate(msgtokens[2], msg);
                } else { // Forward to the ring successor and wait for response from him
                    try {
                        Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                        String msgToSend = message + "\n";

                        OutputStream out = s.getOutputStream();
                        DataOutputStream dos = new DataOutputStream(out);
                        dos.writeBytes(msgToSend);
                        dos.flush();
                        // Receive response
                        BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                        String receive = br.readLine();
                        //Forward it to the one who queried initially
                        ps.println(receive); // keyval
                    } catch(UnknownHostException e) {
                        Log.e(TAG, "UnknownHostException");
                    } catch(IOException e) {
                        Log.e(TAG, "IOException");
                    } catch(Exception e) {
                        Log.e(TAG, "Generic Exception");
                    }
                }
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

    protected  void addToRingNew(String port, PrintStream ps) {
        Log.e(TAG, "Entering add to Ring new for port : " + port);
        int index = -1;
        for(int i = 0; i < n.size(); i++)
        {
            Log.e(TAG, "Port at index:" + i +" " + n.get(i).portStr);
            if(port.compareTo(n.get(i).portStr) == 0) {
                index = i;
                break;
            }
        }
        Log.e(TAG, "Index of the joining node: " + index);
        n.get(index).joined = 1;
        // Start traversing backwards from index to find the predecessor
        int i;
        if(index == 0)
            i = n.size()-1;
        else
            i = index-1;

        while(n.get(i).joined != 1) {
            Log.e(TAG, "Entering while loop" + " " + n.get(i).portStr + "-" +n.get(i).joined);
            if(i == 0) {
                i = n.size()-1;
                continue;
            }
            i--;
        }
        // Here we found the predecessor
        String predecessor = n.get(i).portStr;

        // Start traversing forward from index to find the successor
        int j;
        if(index == n.size()-1)
            j = 0;
        else
            j = index + 1;
        while(n.get(j).joined != 1) {
            Log.e(TAG, "Entering while loop" + " " + n.get(j).portStr + "-" +n.get(j).joined);
            if(j == n.size()-1) {
                j = 0;
                continue;
            }
            j++;
        }

        // Here we found the successor
        String successor = n.get(j).portStr;
        String msg = "PREDSUCC" + "-" + predecessor + "-" + successor;
        Log.e(TAG, "Update successor: " + successor + " " + "predecessor:" + predecessor + " for node:" + port);
        ps.println(msg); // update predecessor and successor

        if(predecessor.compareTo("11108") == 0) {
            succPort = port;
            succNode = portToHashes.get(port);
            Log.e(TAG, "Update successor for 11108 to : " + succPort);
        } else {
            String msg1 = "SUCC" + "-" + port;
            sendUpdate(predecessor, msg1);
        }

        if(successor.compareTo("11108") == 0) {
            predPort = port;
            predNode = portToHashes.get(port);
            Log.e(TAG, "Update predecessor for 11108 to : " + predPort);
        } else {
            String msg2 = "PRED" + "-" + port;
            sendUpdate(successor, msg2);
        }
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

    protected MatrixCursor getQueryResponse(String msg, MatrixCursor cur) {
        Log.e(TAG,"getQueryResponse");
        try {
            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
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
            Log.e("sendUodate", "UnknownHostException");
        } catch(IOException e) {
            Log.e("sendUpdate", "IOException");
        } catch(Exception e) {
            Log.e("sendUpdate", "Generic Exception");
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
            if((succNode == null && predNode == null)) {
                // I am the only node in the ring
                Log.v("insert", "Creating file " + filename);
                String string = values.get("value").toString();
                FileOutputStream outputStream;

                outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                insertedKeyList.add(values.get("key").toString());
                Log.v("insert", "Successfully inserted " + values.get("value").toString());

            } else if((/*node.compareTo(predNode) > 0 &&*/
                    node.compareTo(filename) >= 0 && predNode.compareTo(filename) < 0) ||
                    (node.compareTo(predNode) < 0 && predNode.compareTo(filename) <= 0 && node.compareTo(filename) <=0) ||
                    (node.compareTo(predNode) < 0 && node.compareTo(filename) >= 0 && predNode.compareTo(filename) >= 0 )
                ){
                Log.v("insert", "Creating file " + filename);
                String string = values.get("value").toString();
                FileOutputStream outputStream;

                outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                insertedKeyList.add(values.get("key").toString());
                Log.v("insert", "Successfully inserted " + values.get("value").toString());

            } else { // Sends it to the succNode
                String msg = "INSERT" + "-" + values.get("key").toString() + "-" + values.get("value").toString();
                sendUpdate(succPort, msg);
            }
        } catch(Exception e) {
            Log.e("insert", "File write failed " + e.toString());
        }
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
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
                if((succNode == null && predNode == null) ||
                        (node.compareTo(filename) >= 0 && predNode.compareTo(filename) < 0) ||
                        (node.compareTo(predNode) < 0 && predNode.compareTo(filename) <= 0 && node.compareTo(filename) <=0) ||
                        (node.compareTo(predNode) < 0 && node.compareTo(filename) >= 0 && predNode.compareTo(filename) >= 0))
                {
                    // Less than my hash
                    //and greater than my predecessor hash It belongs to me
                    FileInputStream inputStream;

                    inputStream = getContext().openFileInput(filename);
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String value = bufferedReader.readLine();
                    Log.e("query", "Value for Selection " + filename + " is " + value);
                    //String[] col = {"key", "value"};
                    String[] row = {selection, value};
                    //cursor = new MatrixCursor(col);
                    cursor.addRow(row);
                } else { // Forward to successor
                    String msg = "QUERY" + "-" + selection + "-" + nodePort;
                    return getQueryResponse(msg, cursor);
                }
            }
        } catch(Exception e) {
            Log.e("query", "Exception in querying");
        }
        return cursor;
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
                }
                insertedKeyList.clear();
                if(selection.compareTo("*") == 0)
                {
                    starDeleteQuery();
                }
            } else {
                String filename = genHash(selection);
                if((succNode == null && predNode == null) ||
                        (node.compareTo(filename) >= 0 && predNode.compareTo(filename) < 0) ||
                        (node.compareTo(predNode) < 0 && predNode.compareTo(filename) <= 0 && node.compareTo(filename) <=0) ||
                        (node.compareTo(predNode) < 0 && node.compareTo(filename) >= 0 && predNode.compareTo(filename) >= 0))
                {
                    boolean res = getContext().deleteFile(filename);
                    Log.e("delete", "Deleted successfully" + filename);
                    insertedKeyList.remove(selection);
                } else { // Forward to successor
                    String msg = "DELETE" + "-" + selection;
                    sendUpdate(succPort, msg);
                }
            }
        } catch(Exception e) {
            Log.e("delete", "Exception in deleting");
        }
        return 0;
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