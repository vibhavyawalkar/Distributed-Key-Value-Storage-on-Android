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
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.*;
import java.util.HashMap;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String [] portList = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    HashMap<String, String> portHash = new HashMap<String, String>();
    static ArrayList<String> insertedKeyList = new ArrayList<String>();
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
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() -4);
        final String myPort = String.valueOf(Integer.parseInt(portStr)*2);
        nodePort = myPort;
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(IOException e) {
            Log.e(TAG, "Can't create a ServerSocket!");
            return false;
        }
        try {
            node = genHash(portStr);
        } catch(NoSuchAlgorithmException e) {
            Log.e(TAG, "NO such algorithm exception");
        }

        Log.e(TAG, "Preparing to send join request");
        if(!myPort.equals("11108")) {
            String msg = "JOIN" + "-" + myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        } else {
            Log.e(TAG, "My hash: " + node);
        }

        return true;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                String msgToSend = msgs[0].trim() + "\n";
                // Send Joining request
                OutputStream out = s.getOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeBytes(msgToSend);
                dos.flush();

                // Receive response
                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String receive = br.readLine();
                //Update the successor and predecessor

            } catch(UnknownHostException e) {
                Log.e(TAG, "ClientSide UnknownHostException");
            } catch(IOException e) {
                Log.e(TAG, "ClientSide IOException");
            } catch(Exception e) {
                Log.e(TAG, "ClientSide Generic Exception");
            }
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
                    // Read data from the client
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    // Send data to the client
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
        Log.e(TAG, "Entering handleRequest");
        String [] msgtokens = message.split("-", 2);

        Log.e(TAG, "Request from client " + msgtokens[0]);
        if(msgtokens[0].equalsIgnoreCase("JOIN")){
            try {
                String nodePort = msgtokens[1];
                String nodeHash = genHash(String.valueOf(Integer.parseInt(nodePort)/2));
                addToRing(nodePort, nodeHash);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "ServerSide no such algorithm exception");
            } catch (Exception e) {
                Log.e(TAG, "ServerSide generic exception");
            }

        } else if(msgtokens[0].equalsIgnoreCase("PRED")) {
            predNode = msgtokens[1];
        } else if(msgtokens[0].equalsIgnoreCase("SUCC")) {
            succNode = msgtokens[1];
        } else if(msgtokens[0].equalsIgnoreCase("INSERT")) {
            ContentValues values = new ContentValues();
            values.put(msgtokens[1], msgtokens[2]);
            insert(mUri, values);
        } else if(msgtokens[0].equalsIgnoreCase("QUERY")) {
            try {
                String filename = genHash("DHT" + msgtokens[1]);
                if(node.compareTo(filename) > 0 && predNode.compareTo(filename) < 0) {
                    String [] col = {"key", "value"};
                    MatrixCursor cursor_ = new MatrixCursor(col);
                    cursor_ = (MatrixCursor)query(mUri, null, msgtokens[1], null, null);
                    String msg = "QUERYRES" + "-" + cursor_.toString();
                    sendUpdate(msgtokens[2], msg);
                } else { // Forward to the ring successor
                    sendUpdate(succPort, message);
                }
            } catch(NoSuchAlgorithmException e) {
                Log.e(TAG, "No such algo exception");
            }

        } else if(msgtokens[0].equalsIgnoreCase("QUERY*")) {
            if(msgtokens[1] == nodePort) { // I initiated the query, now I should have the final cursor
            } else  { // Add my entire cursor and forward to my successor

            }
        }

        String msgToSend = "ack"+ "\n";
        ps.println(msgToSend);

        Log.e(TAG, "PredHash:" + predNode +" " + "SuccHash:" + succNode);
        return;

    }

    protected void addToRing(String port, String hash) {
        Log.e(TAG, "Entering addToRing " + port + " " +hash);
        if(node.compareTo(hash) > 0) { // Less than my hash
            Log.e(TAG, hash + " less than " + node);
            if(predNode == null) {
                Log.e(TAG, "Checkpoint 2");
                predNode = hash;
                predPort = port;
                //add to hash port map

            } else if(predNode.compareTo(hash) < 0 ){ // less than my hash but greater than pred
                //Lies between me and predecessor
                Log.e(TAG, "Checkpoint 3");
                String tmpNode = predNode;
                String tmpPort = predPort;
                predNode = hash;
                predPort = port;
                // add to hash port map
                String msg = "SUCC" + "-" + hash;
                sendUpdate(tmpPort, msg); // Update successor
                // Send message to the joining node to update its successor and predecessor
                String msg1 = "SUCC" + "-" + node; // myhash
                sendUpdate(predPort, msg1);
                String msg2 = "PRED" + "-" + tmpNode;
                sendUpdate(predPort, msg2);
            } else { // Forward joining request to the predecessor
                Log.e(TAG, "Checkpoint 4");
                String msg = "JOIN" + "-" + port;
                sendUpdate(predPort, msg);
            }

        } else if(node.compareTo(hash) < 0) { // Greater than my hash
            Log.e(TAG, hash + "  than " + node);
            if(succNode == null) {
                Log.e(TAG, "Checkpoint 5");
                succNode = hash;
                succPort = port;
                // Add to hash port map
            } else if(succNode.compareTo(hash) > 0) { // greater than my hash but less than succ
                // Lies between me and succ
                Log.e(TAG, "Checkpoint 6");
                String tmpNode = succNode;
                String tmpPort = succPort;
                succNode = hash;
                succPort = port;
                // add to hash port map
                String msg = "PRED" + "-" + hash;
                sendUpdate(tmpPort, msg); // Update predecessor
                // Send message to the joining node to update its successor and predecessor
                String msg1 = "SUCC" + "-" + tmpNode;
                sendUpdate(succPort, msg1);
                String msg2 = "PRED" + "-" + node;
                sendUpdate(succPort, msg2);
            } else { // Forward joining request to successor
                String msg = "JOIN" + "-" + port;
                Log.e(TAG, "Checkpoint 7");
                sendUpdate(succPort, msg);
            }
        }
        Log.e(TAG, "Successfully added to the ring");
    }

    protected void sendUpdate(String port, String msg) {
        Log.e(TAG,"Entering sendUpdate");
        try {
            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
            String msgToSend = msg + "\n";
            // Send Joining request
            OutputStream out = s.getOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeBytes(msgToSend);
            dos.flush();

            // Receive response
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String receive = br.readLine();
            //Update the successor and predecessor
        } catch(UnknownHostException e) {
            Log.e(TAG, "ClientSide UnknownHostException");
        } catch(IOException e) {
            Log.e(TAG, "ClientSide IOException");
        } catch(Exception e) {
            Log.e(TAG, "ClientSide Generic Exception");
        }
        return;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            String filename = genHash("DHT" + values.get("key").toString());
            if(node.compareTo(filename) > 0 && predNode.compareTo(filename) < 0) { // less than me and greater than my pred, then it lies between me and my pred
                // so, it belongs to me

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
        MatrixCursor cc = new MatrixCursor(col);
        try {
            if (selection.compareTo("@") == 0 || selection.compareTo("*") == 0) { // all local
                Log.e(TAG, "Query is @");
                for(int i = 0; i < insertedKeyList.size(); i++) {
                    String filename = genHash( "DHT" + insertedKeyList.get(i));
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
                    String msg = "QUERY*" + "-" + nodePort + "-" + cursor.toString();
                    sendUpdate(succPort,msg);
                }
            } else {
                String filename = genHash("DHT" + selection);
                if(node.compareTo(filename) > 0 && predNode.compareTo(filename) < 0) { // Less than my hash
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
                    sendUpdate(succNode, msg);
                }
            }
        } catch(Exception e) {
            Log.e("query", "Exception in querying");
        }
        return cursor;
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
