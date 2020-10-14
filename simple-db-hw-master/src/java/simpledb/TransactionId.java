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

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime() {
        Random random= new Random();
        this.beginTime = System.currentTimeMillis()+random.nextInt(1000);;
    }

    public TransactionId() {
        myid = counter.getAndIncrement();
        Random random= new Random();
        beginTime=System.currentTimeMillis()+random.nextInt(1000);
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
