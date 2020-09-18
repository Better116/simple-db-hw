package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private List<Tuple> tuples;
    private int fieldNumGroupOrNot=0;

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
        if(what.toString()!="count")throw new IllegalArgumentException();
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        tuples=new ArrayList<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(tup==null)return;
        String operator=what.toString();
        int i;
        String aggregateVal="aggName("+operator+")("+tup.getField(afield).toString()+")";

        if(tuples.size()==0){
            TupleDesc tupleDesc;
            if(gbfield==Aggregator.NO_GROUPING){tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{aggregateVal});fieldNumGroupOrNot=0;}
            else {tupleDesc=new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupVal",aggregateVal});fieldNumGroupOrNot=1;}
            Tuple t=new Tuple(tupleDesc);
            if(fieldNumGroupOrNot==1)t.setField(0,tup.getField(gbfield));
            t.setField(fieldNumGroupOrNot,new IntField(1));
            tuples.add(t);
        }
        else {
            for(i=0;i<tuples.size();i++){
                Tuple aTuple=tuples.get(i);//聚合后的tuple
                if(gbfield==Aggregator.NO_GROUPING || tup.getField(gbfield).equals(aTuple.getField(0))){
                    aTuple.setField(fieldNumGroupOrNot,new IntField(Integer.parseInt(aTuple.getField(fieldNumGroupOrNot).toString())+1));
                    return;
                }
            }
            if(gbfield!=Aggregator.NO_GROUPING && i==tuples.size()){
                TupleDesc tupleDesc=new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupVal",aggregateVal});
                Tuple t=new Tuple(tupleDesc);
                t.setField(0,tup.getField(gbfield));
                t.setField(1,new IntField(1));
                tuples.add(t);
            }
        }
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
        return new OpIterator() {
            boolean isOpen=false;
            int cursor=0;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen=true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen)return false;
                if(cursor<tuples.size())return true;
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext())throw  new NoSuchElementException();
                return tuples.get(cursor++);
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                cursor=0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tuples.get(cursor).getTupleDesc();
            }

            @Override
            public void close() {
                cursor=0;
                isOpen=false;
            }
        };
    }

}
