package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;


    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private Map<PageId,BufferPage> buffermap;


    private class BufferPage{
//        ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
        PageLock lock=new PageLock();
        Page page;
        TransactionId transactionId;
        Permissions permissions;
        int numberOfUsed;//用来换页

        public int getNumberOfUsed() {
            return numberOfUsed;
        }

        public void setNumberOfUsed(int numberOfUsed) {
            this.numberOfUsed = numberOfUsed;
        }

        public BufferPage(Page page, TransactionId transactionId, Permissions permissions) {
            this.page = page;
            this.transactionId = transactionId;
            this.permissions = permissions;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        this.buffermap=new ConcurrentHashMap<>(numPages);
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
        if (!buffermap.containsKey(pid) && buffermap.size()>=numPages){
            evictPage();
        }
        if (!buffermap.containsKey(pid)) {
            Catalog catalog=Database.getCatalog();
            HeapFile heapFile=(HeapFile)(catalog.getDatabaseFile(pid.getTableId()));
            Page page=heapFile.readPage(pid);
            BufferPage bufferPage=new BufferPage(page,tid,perm);
            buffermap.put(pid,bufferPage);
        }

        //同一个事务对同一个页既可以获取写锁，也可以获取读锁，即写读不互斥，所以ReentrantReadWriteLock（读读不互斥、读写互斥、写写互斥）用不了
        buffermap.get(pid).lock.getLock(pid,tid,perm);
        Page page=buffermap.get(pid).page;
        return page;
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
        BufferPage bufferPage=buffermap.get(pid);
        if(bufferPage.transactionId!=null && tid.equals(bufferPage.transactionId)){
            bufferPage.lock.releasePage(tid,pid);
        }

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //刷新脏页到磁盘、释放锁
        flushPages(tid);
        for (Map.Entry entry:buffermap.entrySet()
             ) {
            BufferPage bufferPage=(BufferPage) entry.getValue();
            if(bufferPage.transactionId!=null && bufferPage.transactionId.equals(tid))releasePage(tid,(PageId) entry.getKey());
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        BufferPage bufferPage=buffermap.get(p);
        return bufferPage.lock.holdsLock(tid,p);
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
        if(commit)transactionComplete(tid);
        else{
            //中止，页面恢复到磁盘状态、释放锁
            for (Map.Entry entry:buffermap.entrySet()
                    ) {
                BufferPage bufferPage=(BufferPage) entry.getValue();
                if(bufferPage.transactionId!=null && bufferPage.transactionId.equals(tid)){
                    releasePage(tid,(PageId) entry.getKey());
                    discardPage((PageId) entry.getKey());
                }
            }
        }
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
    public void insertTuple(TransactionId tid, int tableId, Tuple t)  //添加新页时直接添加进磁盘了，事务不一定提交
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
       // HeapFile heapFile=(HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        DbFile heapFile= Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages=heapFile.insertTuple(tid,t);
        //PageId pid=pages.get(0).getId();
        for(int i=0;i<pages.size();i++){
            PageId pid=pages.get(i).getId();
            BufferPage bufferPage=new BufferPage(pages.get(0),tid,Permissions.READ_ONLY);
            bufferPage.setNumberOfUsed(bufferPage.getNumberOfUsed()+1);
            if(buffermap.get(pid)==null && buffermap.size()>=numPages){
                evictPage();
            }
            buffermap.put(pid,bufferPage);
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
        PageId pageId=t.getRecordId().getPageId();

        HeapFile heapFile=(HeapFile) Database.getCatalog().getDatabaseFile(pageId.getTableId());
        ArrayList<Page> pages=heapFile.deleteTuple(tid,t);
        BufferPage bufferPage=buffermap.get(pageId);
        bufferPage.setNumberOfUsed(bufferPage.getNumberOfUsed()+1);
        //把修改过的那一页换进来,heapfile中没有存数据，操作的就是bufferpool里的对象
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Map.Entry<PageId,BufferPage> entry: buffermap.entrySet()){
            flushPage(entry.getKey());
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
        buffermap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            DbFile heapFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=getPage(buffermap.get(pid).transactionId, pid, Permissions.READ_ONLY);
            TransactionId tid=page.isDirty();
            if(tid!=null) Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false,tid);
            buffermap.put(pid,new BufferPage(page,tid,Permissions.READ_ONLY));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry entry:buffermap.entrySet()
             ) {
            BufferPage bufferPage=(BufferPage)(entry.getValue());
            if(bufferPage.transactionId!=null && tid.equals(bufferPage.transactionId))flushPage((PageId) entry.getKey());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    //不驱逐脏页
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        /*int min=Integer.MAX_VALUE;
        PageId deletePageId=null;
        for (PageId pageId : buffermap.keySet()) {
            if(buffermap.get(pageId).page.isDirty()!=null)continue;
            if(min>buffermap.get(pageId).getNumberOfUsed()){
                min=buffermap.get(pageId).getNumberOfUsed();
                deletePageId=pageId;
            }
        }
        if(deletePageId==null)throw new DbException("The bufferpool is full of dirty pages! Please first flush!");
        discardPage(deletePageId);
        for (PageId pageId : buffermap.keySet()) {
            BufferPage bufferPage=buffermap.get(pageId);
            bufferPage.setNumberOfUsed(bufferPage.getNumberOfUsed()-min);
        }*/
        for (PageId pageId : buffermap.keySet()) {
            if(buffermap.get(pageId).page.isDirty()!=null)continue;
            buffermap.remove(pageId);
            return;
        }
        throw new DbException("The bufferpool is full of dirty pages! Please first flush!");
    }

}
