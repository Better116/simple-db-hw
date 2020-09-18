package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple tuple1=null;
    private Tuple tuple2;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p=p;
        this.child1=child1;
        this.child2=child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int i;
        if(tuple1==null && child1.hasNext())tuple1=child1.next();
        while (child2.hasNext()){
            if(p.filter(tuple1,(tuple2=child2.next()))){
                TupleDesc td=getTupleDesc();
                Tuple t=new Tuple(td);
                for(i=0;i<child1.getTupleDesc().numFields();i++){
                    t.setField(i,tuple1.getField(i));

                }
                for(int j=0;j<child2.getTupleDesc().numFields();j++){
                    t.setField(i++,tuple2.getField(j));
                }
                return t;
            }
        }
        child2.rewind();
        if(child1.hasNext()){tuple1=null;return fetchNext();}

//        while(child1.hasNext()){
//            tuple1=child1.next();        //每次进来表1都会找下一个元素
//            while (child2.hasNext()){
//                if(p.filter(tuple1,(tuple2=child2.next()))){
//                    System.out.println(true);
//                    TupleDesc td=getTupleDesc();
//                    Tuple t=new Tuple(td);
//                    for(i=0;i<child1.getTupleDesc().numFields();i++){
//                        t.setField(i,tuple1.getField(i));
//
//                    }
//                    for(int j=0;j<child2.getTupleDesc().numFields();j++){
//                        t.setField(i++,tuple2.getField(j));
//                    }
//                    return t;
//                }
//            }
//            child2.rewind();
//        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if(children.length==0){child1=null;child2=null;return;}
        if(children.length==1){child1=children[0];child2=children[1];return;}
        child1=children[0];
        child2=children[1];
    }

}