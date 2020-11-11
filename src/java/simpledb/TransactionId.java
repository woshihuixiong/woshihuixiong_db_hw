package simpledb;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    static AtomicLong counter = new AtomicLong(0);
    final long myid;
    long beginTime;

    public TransactionId() {
    	myid = counter.getAndIncrement();
		/*
		 * 如果每个线程超时时间设置一样，
		 * 那么两个同时开启的会造成死锁的线程也会同时超时然后同时abort，
		 * 然后就会再次同时开启，一直死锁
		 * 加入一个随机数相当于把时间限制设置的不一样
		*/
    	beginTime = System.currentTimeMillis() + new Random().nextInt(100);
    }

    public void resetBeginTime(){
    	beginTime = System.currentTimeMillis() + new Random().nextInt(2000);
	}

    public long getId() {
        return myid;
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionId other = (TransactionId) obj;
		if (myid != other.myid)
			return false;
		return true;
	}

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (myid ^ (myid >>> 32));
		return result;
	}
}
