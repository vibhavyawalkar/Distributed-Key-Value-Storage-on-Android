package edu.buffalo.cse.cse486586.groupmessenger1;

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
            if(t.msg == a.msg) {
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
        Msg a = (Msg) obj;
        Iterator itr = this.iterator();
        while(itr.hasNext()) {
            Msg t = (Msg)itr.next();
            if(t.msg == a.msg) {
                return super.remove(t);
            }
        }
        return false;
    }
}
