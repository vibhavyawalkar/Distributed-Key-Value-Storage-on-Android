package edu.buffalo.cse.cse486586.groupmessenger2;

import android.util.Log;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class pQueue extends PriorityBlockingQueue<Msg> {

    public pQueue(Comparator<Msg> a)
    {
        super(1000, a);
    }

    @Override
    public boolean contains(Object obj) {
        if(!(obj instanceof Msg)) {
            return false;
        }
        Msg a = (Msg) obj;
        Iterator itr = this.iterator();
        while(itr.hasNext()) {
            Msg t = (Msg)itr.next();
            if(t.msg.compareTo(a.msg) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean remove(Object obj) {
        if(!(obj instanceof Msg)) {
            return false;
        }
        Msg x = (Msg) obj;
        Log.e("Qremove", "Message to remove from the queue:" + x.msg + "$");
        Iterator itr = this.iterator();
        while(itr.hasNext()) {
            Msg t = (Msg)itr.next();
            Log.e("Qremove", "Iterating the queue:" + t.msg + "$");
            if(x.msg.compareTo(t.msg) == 0)
            {
                Log.e("Qremove", "Message found in queue, now removing!" + x.msg + " " + t.msg);
                super.remove(t);
                return true;
            }
        }
        return false;
    }
}
