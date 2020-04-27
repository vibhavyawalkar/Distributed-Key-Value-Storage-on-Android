package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.buffalo.cse.cse486586.groupmessenger2.R;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

class MsgComparator implements Comparator<Msg> {
    @Override
    public int compare(Msg a, Msg b)
    {
        if(a.sequenceNumber_part1 == b.sequenceNumber_part1) {
            if (a.sequenceNumber_part2 < b.sequenceNumber_part2) {
                return -1;
            } else if(a.sequenceNumber_part2 > b.sequenceNumber_part2) {
                return 1;
            }
        } else {
            if (a.sequenceNumber_part1 < b.sequenceNumber_part1) {
                return -1;
            } else if (a.sequenceNumber_part1 > b.sequenceNumber_part1) {
                return 1;
            }
        }
        return 0;
    }
}

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String [] portList = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static private volatile int sequence_no = -1;
    static int pid = android.os.Process.myPid();

    HashMap<String, Integer> responseMap = new HashMap<String, Integer>();
    private final GroupMessengerProvider provider = new GroupMessengerProvider();
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

    pQueue myQueue = new pQueue(new MsgComparator());

    //PriorityBlockingQueue myQueue = new PriorityBlockingQueue<Msg>(1000, new MsgComparator() );
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected synchronized void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf(Integer.parseInt(portStr)*2);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(IOException e) {
            Log.e(TAG, "Can't create a ServerSocket!");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final EditText editText = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                //String msg = editText.getText().toString() + "\n";
                String msg = editText.getText().toString();
                editText.setText("");
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                return;
            }
        });
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        public Socket socket;
        private volatile int proposedSeq = 0;
        public int agreedUpon = -1;
        private final Lock lock = new ReentrantLock();


        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Log.e(TAG, "Server Running");
                    socket = serverSocket.accept();
                    //socket.setSoTimeout(500);
                    Log.e(TAG, "Server accepted connection");
                    // To read data from the client
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    // To send data to the client
                    PrintStream ps = new PrintStream(socket.getOutputStream());
                    String receive = "";
                    lock.lock();
                    if((receive = br.readLine()) != null) {
                        Log.e(TAG, "Data received by the server " + receive);
                        orderMessages(receive, br, ps);
                        String display = "Sequence received from the client " + receive;
                        lock.unlock();
                        publishProgress(display);

                        Log.e(TAG, "Done with publishProgress ");
                    } else {
                        Log.e(TAG, "NULL data received from the client");
                        lock.unlock();
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Socket IO Exception");
                lock.unlock();
            } catch (Exception e) {
                Log.e(TAG, "Generic Server Exception");
                lock.unlock();
            }
            return null;
        }

        // This method will send the servers proposal and
        protected synchronized void orderMessages(String message, BufferedReader br, PrintStream ps)
        {
            Log.e(TAG, "Entering orderMessages");
            String [] msgtokens = message.split("-", 2);

            Log.e(TAG, "Message: " + msgtokens[0]);

           /* if(!responseMap.containsKey(msgtokens[0])) {
                responseMap.put(msgtokens[0], 1);

                Log.e(TAG, "Heard the sequence number for the first time");

                proposedSeq = Math.max(proposedSeq, agreedUpon) + 1;
             */
             proposedSeq = proposedSeq + 1;

                /* This should be max of previously seen sequence number and
                previously agreed upon sequence number + 1*/

                Msg a = new Msg(msgtokens[0], proposedSeq, pid, false);
                myQueue.add(a);

                String msgToSend = msgtokens[0] + "-" + proposedSeq + "-" + pid + "\n";
                Log.e(TAG, "Sent proposal to the client " + msgToSend);
                String displayMsg = "Sent proposal to client " + msgToSend;
                ps.println(msgToSend);
                publishProgress(displayMsg);
            //} else {

                String finalPriority ="";
                try {
                    if ((finalPriority = br.readLine()) != null) {
                        String [] msgtokens_ = finalPriority.split("-", 2);
                        String [] seq_pid = msgtokens_[1].split("-", 2);

                        int seq_part1 = Integer.parseInt(seq_pid[0]);
                        int seq_part2 = Integer.parseInt(seq_pid[1]);

                        Log.e(TAG, "Heard the final agreed sequence number");
                        Log.e(TAG, "SEQ:" + msgtokens_[1]);
                        agreedUpon = seq_part1;

                        Log.e(TAG, myQueue.toString());


                        Msg b = new Msg(msgtokens_[0], seq_part1, seq_part2, true);
                        myQueue.remove(b);
                        Log.e(TAG, myQueue.toString());
                        myQueue.add(b);
                        Log.e(TAG, myQueue.toString());

                        sequence_no++;
                        ContentValues content = new ContentValues();
                        Log.e(TAG, "key --> " + sequence_no);
                        Log.e(TAG, "val --> " + b.msg);
                        content.put("key", Integer.toString(sequence_no));
                        content.put("value", b.msg);
                        provider.setProviderContext(getApplicationContext());
                        provider.insert(mUri, content);

                        Log.e(TAG, "Delivered Successfully");
                    } else {
                        Log.e(TAG, "Found null data");
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Exception reading the final priority!");
                    //lock.unlock();
                } catch(Exception e) {
                    Log.e(TAG, "Generic Exception at server receiving the final priority");
                    //lock.unlock();
                }
/*

                */
                /* Add to the content provider, with my sequence number: 'sequence_no' */
              //deliverMsg();
/*
            sequence_no++;
            ContentValues content = new ContentValues();
            Log.e(TAG, "key --> " + sequence_no);
            Log.e(TAG, "val --> " + b.msg);
            content.put("key", Integer.toString(sequence_no));
            content.put("value", b.msg);
            provider.setProviderContext(getApplicationContext());
            provider.insert(mUri, content);

                Log.e(TAG, "Delivered Successfully"); */
            //}
        }

        protected synchronized void deliverMsg() {
            while (!myQueue.isEmpty()) {
                Msg a = (Msg)myQueue.peek();
                Log.e(TAG, "Entering the deliverMsg function, a.deliverable:" + a.deliverable);

                if (a.deliverable == true) {
                    Log.e(TAG, "Message is found deliverable, deliver it");
                    Msg del = (Msg)myQueue.poll();
                    sequence_no++;
                    ContentValues content = new ContentValues();
                    Log.e(TAG, "key --> " + sequence_no);
                    Log.e(TAG, "val --> " + del.msg);
                    content.put("key", Integer.toString(sequence_no));
                    content.put("value", del.msg);
                    provider.setProviderContext(getApplicationContext());
                    provider.insert(mUri, content);
                } else {
                    Log.e(TAG, "Done with delivering, deliverable messages");
                    break;
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            //String strReceived = strings[0].trim();
            String strReceived = strings[0];

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        private final Lock lock = new ReentrantLock();
        //Socket [] clientSockets = new Socket[5];
        //OutputStream [] out = new OutputStream[5];
        @Override
        protected synchronized Void doInBackground(String... msgs) {
            String arr[] = new String[portList.length];
            Socket [] clientSockets = new Socket[5];
            DataOutputStream [] dos = new DataOutputStream[5];

            for(int i = 0; i < portList.length; i++) {
                try {
                    String  remotePort = portList[i];

                    Log.e(TAG, "Client:Connecting to port " + remotePort);
                    lock.lock();
                    clientSockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    //clientSockets[i].setSoTimeout(500);
                    String msgToSend = msgs[0].trim() + "\n";
                    Log.e(TAG, "Client: Initial Message to send " + msgToSend);
                    // Send initial message
                    OutputStream out = clientSockets[i].getOutputStream();
                    dos[i] = new DataOutputStream(out);
                    dos[i].writeBytes(msgToSend);
                    dos[i].flush();

                    // Receive proposal from the client
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSockets[i].getInputStream()));
                    String receive = br.readLine();
                    //lock.unlock();
                    Log.e(TAG, "Client:Received proposal from server " + portList[i]);
                    String [] msgtokens = receive.split("-", 2);

                    Log.e(TAG, "Client:Message: " + msgtokens[0] + "SEQ:" + msgtokens[1]);

                    arr[i] = msgtokens[1];

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException1");
                    arr[i] = "0-0";
                    lock.unlock();
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask IOException1");
                    arr[i] = "0-0";
                    lock.unlock();
                } catch (Exception e) {
                    Log.e(TAG, "Generic Client Exception1");
                    arr[i] = "0-0";
                    lock.unlock();
                }
            }
            /* At this stage the client heard back from all the server's the proposed priorities
              and it has decided on the final priority which now needs to be broadcast. */

            String agreedPriority = findFinalPriority(arr);

            String finalMessage = msgs[0].trim() + "-" + agreedPriority + "\n";

            try {
                for(int i = 0; i < portList.length; i++) {

                    String  remotePort = portList[i];

                    Log.e(TAG, "Client:Final priority sending, Connecting to port " + remotePort);
                    //clientSockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            //Integer.parseInt(remotePort));
                    //clientSockets[i].setSoTimeout(2000);
                    Log.e(TAG, "Client:Final priority message to send " + finalMessage);
                    // Send final sequence number with the  message to all the servers
                    //OutputStream out = clientSockets[i].getOutputStream();
                    //DataOutputStream dos = new DataOutputStream(out);

                    dos[i].writeBytes(finalMessage);
                    dos[i].flush();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException2");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask IOException2");
            } catch (Exception e) {
                Log.e(TAG, "Generic Client Exception2");
            }
            return null;
        }

        protected synchronized String findFinalPriority(String [] a) {
            int finalSeq_part1 = -1;
            int finalSeq_part2 = -1;
            String ret = "";
            try {
                for (int i = 0; i < a.length; i++) {
                    String[] seq_pid = a[i].split("-", 2);

                    int seq_part1 = Integer.parseInt(seq_pid[0]);
                    int seq_part2 = Integer.parseInt(seq_pid[1]);

                    if (finalSeq_part1 < seq_part1) {
                        finalSeq_part1 = seq_part1;
                        finalSeq_part2 = seq_part2;
                    } else if (finalSeq_part1 == seq_part1) {
                        if (finalSeq_part2 < seq_part2)
                            finalSeq_part2 = seq_part2;
                    }
                }
                ret = Integer.toString(finalSeq_part1) + "-" + Integer.toString(finalSeq_part2);
                Log.e(TAG, "Client:Final decided priority : " + ret);
            } catch(NullPointerException e) {
                Log.e(TAG, "Client:Null Pointer Exception when calculating final priority!");
            } catch(Exception e) {
                Log.e(TAG, "Client:Exception when calculating final priority");
            }
            return ret;
        }

        protected void onProgressUpdate(String...strings) {
            //String strReceived = strings[0].trim();
            String strReceived = strings[0];

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("" +strReceived + "\t\n");

            return;
        }
    }
}
