package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public TDItem[] tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new Iterator<TDItem>() {
            int iterCur = 0;
            @Override
            public boolean hasNext() {
                return iterCur < tdItems.length;
            }

            @Override
            public TDItem next() {
                if(hasNext()) return tdItems[iterCur++];
                return null;
            }

            @Override
            public void remove(){
                throw new UnsupportedOperationException("This demo Iterator does not implement the remove method");
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(typeAr == null) return;
        tdItems = new TDItem[typeAr.length];
        for(int i=0; i<typeAr.length; i++){
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if(typeAr == null) return;
        tdItems = new TDItem[typeAr.length];
        for(int i=0; i<typeAr.length; i++){
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= tdItems.length || i < 0) throw new NoSuchElementException();
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= tdItems.length || i < 0) throw new NoSuchElementException();
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null) throw new NoSuchElementException();
        for(int i=0; i<tdItems.length; i++){
            if(tdItems[i].fieldName!=null && tdItems[i].fieldName.equals(name)) return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem tdItem : tdItems) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int lengthOne = td1.numFields();
        int lengthTwo = td2.numFields();
        Type[] tt = new Type[lengthOne+lengthTwo];
        String[] ss = new String[lengthOne+lengthTwo];
        for(int i=0; i<lengthOne; i++){
            tt[i] = td1.getFieldType(i);
            ss[i] = td1.getFieldName(i);
        }
        for(int i=0; i<lengthTwo; i++){
            tt[i+lengthOne] = td2.getFieldType(i);
            ss[i+lengthOne] = td2.getFieldName(i);
        }

        return new TupleDesc(tt, ss);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(this == o){
            return true;
        }
        if(o == null || getClass() != o.getClass()) return false;
        TupleDesc tp = (TupleDesc)o;
        if(tp.tdItems == null && tdItems == null) return true;
        if(tp.tdItems == null) return false;
        if(tdItems == null) return false;
        if(tdItems.length != tp.tdItems.length) return false;
        for(int i=0; i<tdItems.length; i++){
            if(!tdItems[i].toString().equals(tp.tdItems[i].toString())) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
        int ans = 0;
        if(tdItems == null || tdItems.length == 0) return "none".hashCode();
        ans ^= tdItems[0].toString().hashCode();
        ans ^= tdItems[tdItems.length-1].toString().hashCode();
        ans ^= tdItems[tdItems.length/2].toString().hashCode();
        return ans;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        if(getSize() == 0 ) return "";
        StringBuilder ans = new StringBuilder();
        for(int i=0; i<tdItems.length; i++){
            ans.append(tdItems[i].fieldType).append("(").append(tdItems[i].fieldName).append(")");
            if(i != tdItems.length-1) ans.append(",");
        }
        return ans.toString();
    }
}
