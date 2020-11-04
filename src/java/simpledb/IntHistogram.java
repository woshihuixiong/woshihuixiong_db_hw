package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    int[] buckets;
    int minInt;
    int maxInt;
    int width;
    int countAll = 0;


    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.minInt = min;
        this.maxInt = max;
        width = (max - min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        countAll++;
        buckets[calculateBucketNum(v)]++;
    }

    private int calculateBucketNum(int v){
        if(v >= minInt+width*(buckets.length-1)) return buckets.length-1;
        return (v-minInt)/width;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if(v < minInt){
            switch (op){
                case EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 0.0;
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 1.0;
            }
        }

        if(v > maxInt){
            switch (op){
                case EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 0.0;
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 1.0;
            }
        }

        int bucketNum = calculateBucketNum(v);
        int bucketWidth = bucketNum == buckets.length-1 ? maxInt-minInt+1-width*(buckets.length-1) : width;

        if(op == Predicate.Op.EQUALS){
            return 1.0*buckets[bucketNum]/bucketWidth/countAll;
        }
        if(op == Predicate.Op.NOT_EQUALS){
            return 1-1.0*buckets[bucketNum]/bucketWidth/countAll;
        }

        double countSmaller = 0;
        double countLarger = 0;
        for(int i=0; i<buckets.length; i++){
            if(bucketNum != i) countSmaller+=buckets[i];
            else break;
        }
        countSmaller += 1.0*buckets[bucketNum]*(v-width*bucketNum-minInt)/bucketWidth;
        countLarger = countAll-countSmaller-1.0*buckets[bucketNum]/bucketWidth;

        return switch (op) {
            case LESS_THAN -> countSmaller / countAll;
            case LESS_THAN_OR_EQ -> 1 - countLarger / countAll;
            case GREATER_THAN -> countLarger / countAll;
            case GREATER_THAN_OR_EQ -> 1 - countSmaller / countAll;
            default -> 0.0;
        };

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double ans = 0.0;
        for(int buc: buckets){
            ans += buc;
        }
        return ans / (maxInt - minInt +1);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return  "IntHistogram{ " +
                "buckets = " + Arrays.toString(buckets) +
                ", min = " + minInt +
                ", max = " + maxInt +
                ", width = " + width +
                " }";
    }
}
