package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//这个类大部分抄的https://github.com/jasonleaster/simple-db里的TransactionManager，建议两个结合起来看，
// 另外，注意TransactionId里的修改

public class TransactionHelp {
    private static final TransactionHelp transactionHelp = new TransactionHelp();
    public static TransactionHelp getTransactionHelp() {
        return transactionHelp;
    }
    private static final long Transaction_Limit_Time = 1000;
    private final Map<PageId, TransactionId> writeLock = new ConcurrentHashMap<>(); //写锁是排它（独占）锁
    private final Map<PageId, Set<TransactionId>> readLock = new ConcurrentHashMap<>(); //读锁是共享锁
    public void reset() {
        synchronized (TransactionHelp.class){
            transactionHelp.writeLock.clear();
            transactionHelp.readLock.clear();

        }
    }
    public void getLock(TransactionId tid ,PageId pid ,Permissions perm) throws TransactionAbortedException{
        if (perm == Permissions.READ_ONLY){//尝试获取读锁
            while (true){
                if (isTimeOut(tid)){
                    dealWithPotentialDeadlocks(tid);
                }
                synchronized (writeLock){
                    if (!writeLock.containsKey(pid) || writeLock.get(pid) == null || writeLock.get(pid).equals(tid)){
                        //该页面没有写锁或者写锁是tid的
                        readLock.computeIfAbsent(pid, k -> new HashSet<>()).add(tid);
                        //判断map.get(pid)是否存在，若不存在则新建一个键值对，然后往set中添加tid
                        return;
                    }
                }
                Thread.yield(); //让出cpu，尝试参与下一次锁的竞争
            }
        }else{//尝试获取写锁
            while (true){
                if (isTimeOut(tid)){
                    dealWithPotentialDeadlocks(tid);
                }
                synchronized (writeLock){
                    if (writeLock.containsKey(pid) && !writeLock.get(pid).equals(tid)){
                        //该页面被其他事务的写锁占据
                    }else {
                        if (readLock.containsKey(pid) && readLock.get(pid)!=null && !readLock.get(pid).isEmpty()){
                            //如果该页面有被读锁占据
                            if(readLock.get(pid).size() == 1 && readLock.get(pid).contains(tid)){
                                //仅被此事务的读锁占据时才获取写锁
                                writeLock.put(pid,tid);
                                return;
                            }
                        }
                        else {
                            //该页面没有被任何读锁占据，直接获取写锁
                            writeLock.put(pid,tid);
                            return;
                        }
                    }
                }
                Thread.yield();
            }
        }
    }

    //该事务在该页面上是否有锁
    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized (writeLock) {
            //检查是否有排它锁（写锁）
            if (writeLock.get(pid) != null && writeLock.get(pid).equals(tid)) {
                return true;
            }

            //检查是否有共享锁（读锁）
            else {
                Set<TransactionId> readTransactions = readLock.get(pid);
                if (readTransactions != null && readTransactions.contains(tid)) return true;
                return false;
            }
        }
    }

    //释放事务tid在页面pid上的所有锁
    public void releasePage(TransactionId tid, PageId pid) {
        if (pid == null || tid == null) {
            return;
        }

        synchronized (writeLock) {
            //释放写锁
            if (writeLock.get(pid) != null && writeLock.get(pid).equals(tid)) {
                writeLock.remove(pid);
            }

            //释放读锁
            Set<TransactionId> sharedTransactions = readLock.get(pid);
            if (sharedTransactions != null) {
                sharedTransactions.remove(tid);
            }
        }
    }

    public boolean isTimeOut(TransactionId tid){
        return System.currentTimeMillis() - tid.beginTime >= Transaction_Limit_Time;
    }

    private void dealWithPotentialDeadlocks(TransactionId tid) throws TransactionAbortedException{
        try {
            Database.getBufferPool().transactionComplete(tid, false);
            tid.resetBeginTime();
        } catch (IOException e) {
            Debug.log("Abort Dead lock failed!! This shouldn't happen");
        }
        throw  new TransactionAbortedException();
    }


    /**
     * Only for debugging
     */
    public void showLocksOnPages() {
        synchronized (writeLock) {
            for (Map.Entry<PageId, TransactionId> group : writeLock.entrySet()) {
                PageId pid = group.getKey();
                TransactionId xlock = group.getValue();
                Debug.log("Page#" + pid.getPageNumber() + " has X-Lock: " + xlock.getId());
            }

            for (Map.Entry<PageId, Set<TransactionId>> group : readLock.entrySet()) {
                PageId pid = group.getKey();
                Set<TransactionId> slocks = group.getValue();
                for (TransactionId slock : slocks) {
                    Debug.log("Page#" + pid.getPageNumber() + " has S-Lock: " + slock.getId());
                }
            }
        }

    }
}
