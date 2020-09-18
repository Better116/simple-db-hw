package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field1;
    private Predicate.Op op;
    private int field2;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1=field1;
        this.op=op;
        this.field2=field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        //比的就是t1  t2大小,t1在前，不要调整t1  t2顺序，
        //这样t1 t2比较的时候不都用的是t2的field number吗，若两个tuple要比的fieldnumber不一样呢，怎么传入t1的field number?
//        Predicate predicate=new Predicate(field2,op,t2.getField(field2));
//        return predicate.filter(t1);
        Predicate predicate=new Predicate(field1,op,t1.getField(field1));
        return predicate.filter(t2);
    }
    
    public int getField1()
    {
        // some code goes here
        return field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
