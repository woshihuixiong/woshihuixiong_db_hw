package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;  //afield是要聚合的的下标
    private Op what;
    private Map<Field, int[]> field_saveValue;  //field_saveValue里的键值是gbfield，数组里面存放了计数、和、最小值、最大值
    private Map<Field, Tuple> field_tuple;  //聚合后的结果放到这里
    private TupleDesc tupleDesc;
    private Type[] typeAr;
    private int CurAggregate;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        field_saveValue = new HashMap<>();
        field_tuple = new HashMap<>();
        //如果没有gbfield，即不需要分类，初始化一列即可，结果放在key=0的field；否则返回两列，第一列（key=0）放gbfield，第二列放结果（key=1）
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    //扫描下一行
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(tupleDesc == null) tupleDesc = new TupleDesc(typeAr);  //初始化tupleDesc
        Field key = gbfield == Aggregator.NO_GROUPING ? new IntField(gbfield) : tup.getField(gbfield);
        int value = ((IntField)tup.getField(afield)).getValue();  //需要merge的值写入value
        int[] orDefault = field_saveValue.getOrDefault(key, new int[]{0, 0, Integer.MAX_VALUE, Integer.MIN_VALUE});  //field_saveValue里的orDefault里面存放了计数、和、最小值、最大值
        //更新orDefault
        orDefault[0]++;
        orDefault[1] += value;
        orDefault[2] = Math.min(orDefault[2],value);
        orDefault[3] = Math.max(orDefault[3],value);
        field_saveValue.put(key,orDefault);  //将更新完的值存入field_saveValue
        Tuple changeTuple = field_tuple.getOrDefault(key, new Tuple(tupleDesc));
        if(gbfield != Aggregator.NO_GROUPING) changeTuple.setField(0,tup.getField(gbfield));
        //使用switch语句使代码更简洁，其实不需要field_saveValue，用很多if else也行，就是代码会长一些
        switch (what) {
            case MIN -> changeTuple.setField(CurAggregate, new IntField(field_saveValue.get(key)[2]));
            case MAX -> changeTuple.setField(CurAggregate, new IntField(field_saveValue.get(key)[3]));
            case SUM -> changeTuple.setField(CurAggregate, new IntField(field_saveValue.get(key)[1]));
            case COUNT -> changeTuple.setField(CurAggregate, new IntField(field_saveValue.get(key)[0]));
            case AVG -> changeTuple.setField(CurAggregate, new IntField(field_saveValue.get(key)[1] / field_saveValue.get(key)[0]));
        }
        //更新field_tuple，如果key原本有值则自动替换新值
        field_tuple.put(key, changeTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
