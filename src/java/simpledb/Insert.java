package simpledb;

import net.sf.antcontrib.logic.condition.IsPropertyFalse;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    int tableID;
    OpIterator child;
    OpIterator[] children;
    Tuple res;
    boolean hasFetched;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException, TransactionAbortedException {
        // some code goes here
        this.tid = t;
        this.tableID = tableId;
        this.child = child;
        hasFetched = false;
        children = new OpIterator[]{child};
        res = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));

        //if(!tupleDesc.equals(dataBaseFile.getTupleDesc())) throw new DbException("TupleDesc of child differs from table into which we are to insert")
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return res.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        hasFetched = false;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(hasFetched) return null;
        hasFetched = true;
        int ans = 0;
        open();
        while (child.hasNext()){
            try{
                Database.getBufferPool().insertTuple(tid, tableID, child.next());
            }catch (IOException e){
                e.printStackTrace();
            }
            ans++;
        }
        res.setField(0, new IntField(ans));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }
}
