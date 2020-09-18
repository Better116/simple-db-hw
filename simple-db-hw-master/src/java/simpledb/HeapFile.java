package simpledb;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 *
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td){
        // some code goes here
        this.f=f;
        this.td=td;

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     *
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     * 也是表id
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try{
            FileInputStream fileInputStream=new FileInputStream(f);
            byte[] b=new byte[BufferPool.getPageSize()];
            fileInputStream.skip(pid.getPageNumber()*BufferPool.getPageSize());
            if(fileInputStream.read(b)!=-1){
                return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()),b);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        throw new IllegalArgumentException();

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {    //不用把变了的page写入到磁盘吗
        // some code goes here
        // not necessary for lab1

        if(page.getId().getPageNumber()>=numPages()){
            //追加数据
            FileOutputStream fileOutputStream=new FileOutputStream(f,true);
            fileOutputStream.write(page.getPageData());
        }
        else{
            //更改数据
            FileOutputStream fileOutputStream=new FileOutputStream(f);
            byte[] content=page.getPageData();
            fileOutputStream.write(content,page.getId().getPageNumber()*BufferPool.getPageSize(),content.length);
            //第二个参数是流的偏移量还是数组的
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
         //insert完了要加入到磁盘文件f中
        return (int)(f.length()%BufferPool.getPageSize()==0 ? f.length()/BufferPool.getPageSize() : f.length()/BufferPool.getPageSize()+1);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        return null;
        // not necessary for lab1
        //返回的是修改的页
        //插入t是随便插的，插完之后改recordid即可
        ArrayList<Page> pages=new ArrayList<>();
        for(int i=0;i<numPages();i++){
            HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots()==0)continue;
            heapPage.insertTuple(t);
            heapPage.markDirty(true,tid);
            heapPage.setVersion(heapPage.getVersion()+1);
            pages.add(heapPage);
            //writePage(heapPage);
            return pages;
        }
        HeapPage newPage=new HeapPage(new HeapPageId(getId(),numPages()+1),HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        newPage.markDirty(true,tid);
        newPage.setVersion(newPage.getVersion()+1);
        pages.add(newPage);
        writePage(newPage);
        return pages;

//        PageId tpid=t.getRecordId().getPageId();
//        HeapPage heapPage=(HeapPage)bufferPool.getPage(tid,tpid,Permissions.READ_WRITE);
//        try {
//            heapPage.insertTuple(t);
//        }catch (DbException e){
//            e.printStackTrace();
//        }
//        pages.set(tpid.getPageNumber(),heapPage);
//        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
//        return null;
        // not necessary for lab1
        ArrayList<Page> pages=new ArrayList<>();
        HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        heapPage.markDirty(true,tid);
        pages.add(heapPage);
        return pages;   //删除tuple不用写入磁盘吗
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            BufferPool bufferPool=Database.getBufferPool();
            Boolean isOpen=false;
            HeapPage page;
            Iterator<Tuple> tuples;
            int pgno=0;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen=true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen)return false;
                if(tuples==null){
                    page=(HeapPage) bufferPool.getPage(tid,new HeapPageId(getId(),pgno),Permissions.READ_ONLY);
                    tuples=page.iterator();
                    pgno++;
                }
                if(tuples.hasNext())return true;
                //tuple不是在连续的page上存储的
                while(pgno<numPages()){
                    page=(HeapPage) bufferPool.getPage(tid,new HeapPageId(getId(),pgno),Permissions.READ_ONLY);
                    tuples=page.iterator();
                    pgno++;
                    if(tuples.hasNext())return true;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isOpen)throw new NoSuchElementException();
                return tuples.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pgno=0;
                tuples=null;//设置为null即可，不用重新让tuples里的cursor重新等于-1
            }

            @Override
            public void close() {
                isOpen=false;
            }
        };

        /*return new DbFileIterator() {
            int numPages = numPages();
            int cur = 0;
            Iterator<Tuple> tupleIterator ;
            boolean isOpen = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen)
                    return false;
                if(tupleIterator == null){
                    HeapPage heap = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cur),Permissions.READ_ONLY);
                    tupleIterator= heap.iterator();
                    cur++;
                }
                if (tupleIterator.hasNext())
                    return true;
                while (cur<numPages){
                    HeapPage heap = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cur),Permissions.READ_ONLY);
                    tupleIterator= heap.iterator();
                    cur++;
                    if (tupleIterator.hasNext())
                        return true;

                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext())
                    return tupleIterator.next();
                throw  new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                cur = 0;
                tupleIterator = null;
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };*/
    }

}

