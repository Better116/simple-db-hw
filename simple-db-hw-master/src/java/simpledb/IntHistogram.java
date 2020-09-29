package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int sizeOfBucket;
    private int buckets;
    private int min;
    private int max;
    private int[] num;
    private int sum;

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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets=buckets;
        this.max=max;
        this.min=min;
        this.sizeOfBucket=((max-min+1)%buckets==0) ? (max-min+1)/buckets : (max-min+1)/buckets+1;


        num=new int[buckets];
//        num[1]=1;            //这只是给了区间，不用算进去
//        num[buckets]=1;
//        sum+=2;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        num[(v-min)/sizeOfBucket]+=1;//左闭右开
        sum+=1;
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
        if(v<min){
            switch(op.toString()){
                case "=":
                case "<":
                case "<=":return 0.0;
                case "<>":
                case ">":
                case ">=":return 1.0;
            }
        }
        if(v>max){
            switch(op.toString()){
                case "=":
                case ">":
                case ">=":return 0.0;
                case "<>":
                case "<":
                case "<=":return 1.0;
            }
        }
        int i=(v-min)/sizeOfBucket;
        if(op.toString().equals("="))return ((double)num[i])/sizeOfBucket/sum;
        if(op.toString().equals("<>"))return 1-((double)num[i])/sizeOfBucket/sum;
        if(op.toString().equals(">") || op.toString().equals("<=")){
            int ceil=(int)Math.ceil((double)(v-min)/sizeOfBucket);
            double part1=(((double)num[i])/sum)*((((double)min+sizeOfBucket*ceil)-v)/sizeOfBucket);
            double part2=0;
            for (i=i+1;i<=buckets -1; i++) {
                part2+=num[i];
            }
            part2=part2/sum;
            if(op.toString().equals(">"))return part1+part2;
            else return 1-part1-part2;
        }
        if(op.toString().equals("<") || op.toString().equals(">=")){
            double part1=(((double)num[i])/sum)*((((double)v)-(min+sizeOfBucket*(i)))/sizeOfBucket);
            double part2=0;
            for (int j=0;j<i ; j++) {
                part2+=num[j];
            }
            part2=part2/sum;
            if(op.toString().equals("<"))return part1+part2;
            else return 1-part1-part2;
        }
        return -1.0;
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

        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s="";
        for (int i = 0; i < buckets; i++) {
            s+=min+sizeOfBucket*i+"~"+min+sizeOfBucket*(i+1)+":"+num[i=1];
        }
        return null;
    }
}
