package edu.buffalo.cse.cse486586.groupmessenger1;

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
import java.nio.Buffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import edu.buffalo.cse.cse486586.groupmessenger1.R;

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
                return 1;
            } else if(a.sequenceNumber_part2 > b.sequenceNumber_part2) {
                return -1;
            }
        } else {
            if(a.sequenceNumber_part1 < b.sequenceNumber_part1) {
                return 1;
            } else if(a.sequenceNumber_part1 > b.sequenceNumber_part1) {
                return -1;
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
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String [] portList = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static int sequence_no = -1;
    static int seq = 0;
    static int pid = android.os.Process.myPid();

    /* If there is an issue with the same message sent again the, we can have the PID
    appended to the messages for breaking ties */
    HashMap<String, Integer> responseMap = new HashMap<String, Integer>();
    HashMap<String, Boolean> generatorMap = new HashMap<String, Boolean>();
    private final GroupMessengerProvider provider = new GroupMessengerProvider();
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    pQueue myQueue = new pQueue(new MsgComparator());

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
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
                // Set the generator flag here
                generatorMap.put(msg, true); // The initial sequence number sent by the client
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort, Integer.toString(seq));
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
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Log.e(TAG, "Server Running");
                    socket = serverSocket.accept();
                    // To read data from the client
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    // To send data to the client
                    PrintStream ps = new PrintStream(socket.getOutputStream());
                    String receive = "";
                    if((receive = br.readLine()) != null) {
                        Log.e(TAG, "Data received by the server " + receive);
                        orderMessages(receive, ps);
                        String display = "Sequence received from the client " + receive;
                        publishProgress(display);

                        Log.e(TAG, "Done with publishProgress ");
                    } else {
                        Log.e(TAG, "NULL data received from the client");
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Socket IO Exception");
            }
            return null;
        }

        // This method will send the servers proposal and
        protected void orderMessages(String message, PrintStream ps)
        {
            Log.e(TAG, "Entering orderMessages");
            String [] msgtokens = message.split("-", 2);

            Log.e(TAG, "Message: " + msgtokens[0] + "SEQ:" + msgtokens[1]);

            String [] seq_pid = msgtokens[1].split("-", 2);

            int seq_part1 = Integer.parseInt(seq_pid[0]);
            int seq_part2 = Integer.parseInt(seq_pid[1]);

            if(!responseMap.containsKey(msgtokens[0]))
                responseMap.put(msgtokens[0], 1);
            else {
                Integer respCount = responseMap.get(msgtokens[0]);
                respCount++;
                responseMap.put(msgtokens[0], respCount);
            }

            /*if(!generatorMap.containsKey(msgtokens[0]))
                generatorMap.put(msgtokens[0], false);

            Log.e(TAG, "Entering orderMessages -- 2");
            Boolean genflag = generatorMap.get(msgtokens[0]);
             */
            Integer responses = responseMap.get(msgtokens[0]);
            if(responses.intValue() == 1) {

                Log.e(TAG, "Heard the sequence number for the first time");
                /* Heard back the final priority from the client,  */
                int proposedSeq;
                if(seq_part1 != seq) {
                    proposedSeq = Math.max(seq_part1, seq) + 1;
                } else {
                    if(seq_part2 == Math.max(seq_part2, pid)) {
                        proposedSeq = seq_part1 + 1 ;
                    } else {
                        proposedSeq = seq + 1;
                    }
                }
                seq++;
                Msg a = new Msg(msgtokens[0], proposedSeq, seq_part2, false);
                myQueue.remove(a);
                myQueue.add(a);

                String msgToSend = msgtokens[0] + "-" + proposedSeq + "-" + pid + "\n";
                Log.e(TAG, "Sent proposal to the client" + msgToSend);
                String displayMsg = "Sent proposal to client " + msgToSend;
                ps.println(msgToSend);
                publishProgress(displayMsg);
            } else if(responses.intValue() == 2) {
                Log.e(TAG, "Heard the final sequence number");
                Msg a = new Msg(msgtokens[0], seq_part1, seq_part2, true);
                myQueue.remove(a);
                myQueue.add(a);
                /* Add to the content provider, with my sequence number: 'sequence_no' */
                deliverMsg();
                Log.e(TAG, "Delivered Successfully");
            }




            // Old code starts here
            /*if(genflag.booleanValue() == true && responses.intValue() == 5)
            { */
                /* If I am the generator of the message and I heard back the proposals from all
                others, then decide the final priority and broadcast it, and mark the message as
                 deliverable in the local pqueue*/
            /*Log.e(TAG, "Case 1");
                int proposedSeq;
                if(seq_part1 != seq) {
                    proposedSeq = Math.max(seq_part1, seq);
                } else {
                    if(seq_part2 == Math.max(seq_part2, pid)) {
                        proposedSeq = seq_part1;
                    } else {
                        proposedSeq = seq;
                    }
                }

                Msg a = new Msg(msgtokens[0], proposedSeq, seq_part2, true);
                myQueue.remove(a);
                myQueue.add(a);

                // Add to the content provider
                deliverMsg();
                String msgToSend = msgtokens[0] + "-" + proposedSeq + "-" + pid + "\n";
                broadcastFinalSeq(msgToSend);

            } else if(genflag.booleanValue() == true && responses.intValue() != 5) { */
                /* Update the priority as per the maximum observed. */
                /*Log.e(TAG, "Case 2");
                int proposedSeq;
                if(seq_part1 != seq) {
                    proposedSeq = Math.max(seq_part1, seq);
                } else {
                    if(seq_part2 == Math.max(seq_part2, pid)) {
                        proposedSeq = seq_part1;
                    } else {
                        proposedSeq = seq;
                    }
                }

                Msg a = new Msg(msgtokens[0], seq_part1, seq_part2, false);
                myQueue.remove(a);
                myQueue.add(a);*/
                /* Just update your observed priority, do nothing else */

            /*} else if(genflag.booleanValue() == false && responses.intValue() == 1) { */
                /* Message arrived at the node for the first time and the message wasn't generated
                by the node, so decide the priority as per the highest sequence number observed so far
                and respond back to the sender with the proposed priority */
                /*Log.e(TAG, "Case 3");
                int proposedSeq;
                if(seq_part1 != seq) {
                    proposedSeq = Math.max(seq_part1, seq);
                } else {
                    if(seq_part2 == Math.max(seq_part2, pid)) {
                        proposedSeq = seq_part1;
                    } else {
                        proposedSeq = seq;
                    }
                }

                Msg a = new Msg(msgtokens[0], proposedSeq, seq_part2, false);
                myQueue.remove(a);
                myQueue.add(a);

                String msgToSend = msgtokens[0] + "-" + proposedSeq + "-" + pid + "\n";
                ps.println(msgToSend);

            } else if(genflag.booleanValue() == false && responses.intValue() == 2) {*/
                /* You heard the final priority from the sender of the message, now update the
                priority of the message and mark the message as deliverable in your priority queue,
                if its deliverable(that is at the front of the queue, then deliver it to the content provider,
                also deliver all the messages which are deliverable after this message in the priority queue
                 */
                //Log.e(TAG, "Case 4");
                /* Add to the content provider, with my sequence number: 'sequence_no' */
                /*deliverMsg();
            }

            Log.e(TAG, "orderMessages -- 3");
*/
        }

        protected void deliverMsg() {
            Msg a;
            do {
                a = myQueue.peek();
                Log.e(TAG, "Entering the deiver function");
                if(a.deliverable == true) {
                    Log.e(TAG, "Message is found deliverable, deliver it");
                    Msg del = myQueue.poll();
                    sequence_no++;
                    ContentValues content =  new ContentValues();
                    Log.e(TAG, "key --> " + sequence_no);
                    Log.e(TAG, "val --> " + del.msg);
                    content.put("key", Integer.toString(sequence_no));
                    content.put("value", del.msg);
                    provider.setProviderContext(getApplicationContext());
                    provider.insert(mUri, content);
                }
            } while(!myQueue.isEmpty() && a.deliverable == true);
            return;
        }

        protected void onProgressUpdate(String...strings) {
            //String strReceived = strings[0].trim();
            String strReceived = strings[0];

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            //remoteTextView.append(msgtokens[0] + " " + msgtokens[1] + "\t\n");
            /*sequence_no++;
            ContentValues content = new ContentValues();
            Log.e(TAG, "key --> " + sequence_no);
            Log.e(TAG, "val --> " + strReceived);
            content.put("key", Integer.toString(sequence_no));
            content.put("value" , strReceived );
            provider.setProviderContext(getApplicationContext());
            provider.insert(mUri, content); */
            return;
        }

        void broadcastFinalSeq(String msg)
        {
            for(int i = 0; i < portList.length; i++) {
                try {
                    String remotePort = portList[i];

                    Log.e(TAG, "Connecting to port " + remotePort);
                    Socket bSock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    OutputStream out = bSock.getOutputStream();
                    DataOutputStream dos = new DataOutputStream(out);
                    dos.writeBytes(msg);
                    dos.flush();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask IOException");
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        //int pid = android.os.Process.myPid();
        Socket [] clientSockets = new Socket[5];
        int finalSeq;
        @Override
        protected Void doInBackground(String... msgs) {
            seq++;
            for(int i = 0; i < portList.length; i++) {
                //if (portList[i] != msgs[1]) {
                try {
                    String  remotePort = portList[i];

                    Log.e(TAG, "Connecting to port " + remotePort);
                    clientSockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    //String msgToSend = msgs[0] + " " + msgs[2];
                    String msgToSend = msgs[0].trim() + "-" + seq + "-" + pid + "\n";
                    Log.e(TAG, "Message to send " + msgToSend);
                    // Send initial message and sequence number
                    OutputStream out = clientSockets[i].getOutputStream();
                    DataOutputStream dos = new DataOutputStream(out);
                    dos.writeBytes(msgToSend);
                    dos.flush();

                    // Receive proposal from the client
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSockets[i].getInputStream()));
                    String receive = br.readLine();

                    Log.e(TAG, "Received proposal from server " + portList[i]);
                    String [] msgtokens = receive.split("-", 2);

                    Log.e(TAG, "Message: " + msgtokens[0] + "SEQ:" + msgtokens[1]);

                    String [] seq_pid = msgtokens[1].split("-", 2);

                    int seq_part1 = Integer.parseInt(seq_pid[0]);
                    int seq_part2 = Integer.parseInt(seq_pid[1]);

                    if(seq_part1 != seq) {
                        finalSeq = Math.max(seq_part1, seq);
                    } else {
                        if(seq_part2 == Math.max(seq_part2, pid)) {
                            finalSeq = seq_part1;
                        } else {
                            finalSeq = seq;
                        }
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask IOException");
                }
                //}
            }
            /* At this stage the client heard back from all the server's the proposed priorities
              and it has decided on the final priority which now needs to be broadcast. */

            for(int i = 0; i < portList.length; i++) {
                //if (portList[i] != msgs[1]) {
                try {
                    String  remotePort = portList[i];

                    Log.e(TAG, "Connecting to port " + remotePort);
                    clientSockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    //String msgToSend = msgs[0] + " " + msgs[2];
                    String msgToSend = msgs[0].trim() + "-" + finalSeq + "-" + pid + "\n";
                    Log.e(TAG, "Message to send " + msgToSend);
                    // Send final sequence number with the  message to all the servers
                    OutputStream out = clientSockets[i].getOutputStream();
                    DataOutputStream dos = new DataOutputStream(out);
                    dos.writeBytes(msgToSend);
                    dos.flush();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask IOException");
                }
                //}
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            //String strReceived = strings[0].trim();
            String strReceived = strings[0];

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("" +strReceived + "\t\n");
            //remoteTextView.append(msgtokens[0] + " " + msgtokens[1] + "\t\n");
            /*sequence_no++;
            ContentValues content = new ContentValues();
            Log.e(TAG, "key --> " + sequence_no);
            Log.e(TAG, "val --> " + strReceived);
            content.put("key", Integer.toString(sequence_no));
            content.put("value" , strReceived );
            provider.setProviderContext(getApplicationContext());
            provider.insert(mUri, content); */
            return;
        }
    }
}
