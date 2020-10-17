package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;  //afield是要聚合的的下标
    private Op what;
    private Map<Field, Tuple> field_tuple;
    private TupleDesc tupleDesc;
    private Type[] typeAr;
    private int CurAggregate;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        field_tuple = new HashMap<>();
        if(gbfield == Aggregator.NO_GROUPING){
            typeAr = new Type[]{Type.INT_TYPE};
            CurAggregate = 0;
        }
        else{
            typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            CurAggregate = 1;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(tupleDesc == null) tupleDesc = new TupleDesc(typeAr);
        Field key = gbfield == Aggregator.NO_GROUPING ? new IntField(gbfield) : tup.getField(gbfield);
        Tuple changeTuple;
        if(field_tuple.containsKey(key)) changeTuple = field_tuple.get(key);
        else{
            changeTuple = new Tuple(tupleDesc);
            changeTuple.setField(CurAggregate, new IntField(0));
        }
        if(gbfield != Aggregator.NO_GROUPING) changeTuple.setField(0, tup.getField(gbfield));
        changeTuple.setField(CurAggregate, new IntField(((IntField)changeTuple.getField(afield)).getValue()+1));
        field_tuple.put(key, changeTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        return new OpIterator() {
            boolean isOpened = false;
            Iterator<Field> fieldIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpened = true;
                fieldIterator = field_tuple.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return isOpened && fieldIterator != null && fieldIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()) return field_tuple.get(fieldIterator.next());
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();

            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                isOpened = false;
                fieldIterator = null;
            }
        };
    }

}
