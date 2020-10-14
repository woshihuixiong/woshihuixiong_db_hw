package simpledb;

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
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file.getAbsoluteFile();
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // BufferPool如果要读取新page就会调用这个方法
    public Page readPage(PageId pid) {
        // some code goes here
        FileInputStream fileInputStream = null;
        HeapPage heapPage = null;
        try{
            //文件输入流，指向file的第一个字节，不是直接把整个文件先读进内存
            fileInputStream = new FileInputStream(file);
            byte[] buff = new byte[BufferPool.getPageSize()];
            //这里调用缓冲池方法只是获取页面大小，与读取页面无直接关联！
            //要从一个文件读取第pid个页面，就要跳过前面的页面，比如读取pid=3页面就要跳过(0,1,2)前面3个页面*页面大小个字节数（调用的是静态方法）
            fileInputStream.skip(pid.getPageNumber()*BufferPool.getPageSize());
            if(fileInputStream.read(buff) != -1){
                heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buff);
            }
        }catch(IOException e){
            e.printStackTrace();
        }finally {
            if(fileInputStream != null){
                try {
                    fileInputStream.close();
                }catch (IOException e){
                    e.printStackTrace();
                    System.out.println("Cannot close fileInputStream!");
                }
            }
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        if(file.length()%BufferPool.getPageSize() == 0) return (int)(file.length()/BufferPool.getPageSize());
        return (int)file.length()/BufferPool.getPageSize()+1;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    //遍历HeapFile中的每个tuple，必须使用BufferPool.getPage()访问HeapFile中的页。此方法将页面加载到缓冲池当中。
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int numPage = numPages();
            int cur = 0;
            Iterator<Tuple> tupleIterator;
            boolean isOpen = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                cur = 0;
                //一开始指向null
                tupleIterator = null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen) return false;
                //如果是一开始，获得第一个页面的迭代器
                if(tupleIterator == null){
                    HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cur),Permissions.READ_ONLY);
                    tupleIterator = heapPage.iterator();
                    cur++;
                }
                //如果当前这个页面的迭代器有下一个tuple则返回true
                if(tupleIterator.hasNext()) return true;
                //如果没有，则寻找下一个hasNext=true的页面的迭代器
                while(cur<numPage){
                    HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cur),Permissions.READ_ONLY);
                    tupleIterator = heapPage.iterator();
                    cur++;
                    if(tupleIterator.hasNext()) return true;
                }
                //找到最后一个页面都没找到，返回false
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()){
                    return tupleIterator.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };
    }

}

