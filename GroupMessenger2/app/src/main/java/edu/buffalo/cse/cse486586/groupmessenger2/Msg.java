package edu.buffalo.cse.cse486586.groupmessenger2;

public class Msg {
    String msg;
    int sequenceNumber_part1;
    int sequenceNumber_part2;
    boolean deliverable;

    public Msg(String message, int seqno_p1, int seqno_p2, boolean deliverableFlag) {
        msg = message;
        sequenceNumber_part1 = seqno_p1;
        sequenceNumber_part2 = seqno_p2;
        deliverable = deliverableFlag;
    }

    public void displayMessage()
    {
        System.out.println(msg + " " + sequenceNumber_part1 + "." + sequenceNumber_part2 + " " + deliverable);
    }
}
