package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private final int numPages;
    private final Map<PageId, Page> bufferMap_PageIDtoPage;

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferMap_PageIDtoPage = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //对该页面尝试加锁，否则阻塞当前线程
        TransactionHelp.getTransactionHelp().getLock(tid, pid, perm);
        //如果缓冲池中没有该页面并且缓冲池已满，则evict一个页面
        while(!bufferMap_PageIDtoPage.containsKey(pid) && bufferMap_PageIDtoPage.size()>=numPages) evictPage();

        Page page = bufferMap_PageIDtoPage.get(pid);
        if(page == null){
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            if(bufferMap_PageIDtoPage.size() >= numPages) evictPage();
            if(perm == Permissions.READ_WRITE) page.markDirty(true, tid);

            bufferMap_PageIDtoPage.put(pid, page);
        }
        return page;

//        if(!bufferMap_PageIDtoPage.containsKey(pid)){
//            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//            bufferMap_PageIDtoPage.put(pid, page);
//        }
//        if(perm == Permissions.READ_WRITE)
//            bufferMap_PageIDtoPage.get(pid).markDirty(true, tid);
//        return bufferMap_PageIDtoPage.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        TransactionHelp.getTransactionHelp().releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) flushPages(tid);
        else{
            for(Map.Entry<PageId, Page> entry: this.bufferMap_PageIDtoPage.entrySet()){
                Page page = entry.getValue();
                PageId pid = entry.getKey();
                if(tid.equals(page.isDirty())) discardPage(pid);
                releasePage(tid, pid);
            }
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return TransactionHelp.getTransactionHelp().holdsLock(tid, p);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dataBaseFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = dataBaseFile.insertTuple(tid, t);
        for(Page page: pages){
            while(!bufferMap_PageIDtoPage.containsKey(page.getId()) && bufferMap_PageIDtoPage.size()>=numPages) evictPage();
            page.markDirty(true, tid);
            bufferMap_PageIDtoPage.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        DbFile dataBaseFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        dataBaseFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId: bufferMap_PageIDtoPage.keySet()){
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if(pid == null) return;
        bufferMap_PageIDtoPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferMap_PageIDtoPage.getOrDefault(pid, null);
        if(page != null && page.isDirty() != null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Map.Entry<PageId, Page> entry: this.bufferMap_PageIDtoPage.entrySet()){
            PageId pid = entry.getKey();
            Page pageToBeFlushed = entry.getValue();
            TransactionId holdTid = pageToBeFlushed.isDirty();

            if(holdTid!=null && holdTid.equals(tid))
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);
            try {
                releasePage(tid, pid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if(bufferMap_PageIDtoPage.size() == 0) throw new DbException("no page to evict");
        for(PageId pageId: bufferMap_PageIDtoPage.keySet()){
            if(bufferMap_PageIDtoPage.get(pageId).isDirty() == null){
                discardPage(pageId);
                return;
            }
        }
        throw new DbException("No page to evict");
    }

}
