package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /*private DbFile file;
    private DbFileIterator dbFileIterator;
    private int numOfTuples;
    private int numOfPages;
    private int ioCostPerPage;
    Object[] histogram;//存储表中每个列的直方图，初始化的时候就计算好，不用每次取某一列时都计算一次
*/
    private int ioCostPerPage;
    private int numOfTuples;
    private int numOfPages;
    Object[] histogram;//每列的直方图数组
    DbFile file;
    TupleDesc tupleDesc;
    DbFileIterator iterator;//这个文件的迭代器
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        /*this.file=Database.getCatalog().getDatabaseFile(tableid);
        this.dbFileIterator=file.iterator(new TransactionId());
        this.ioCostPerPage=ioCostPerPage;
        int numOfFields=file.getTupleDesc().numFields();
        this.histogram=new Object[numOfFields];
        int[] min=new int[numOfFields];
        int[] max=new int[numOfFields];
        TupleDesc tupleDesc=file.getTupleDesc();
        HashSet<PageId> pageid=new HashSet<>();
        for(int i=0;i<min.length;i++){
            min[i]=Integer.MAX_VALUE;
            max[i]=Integer.MIN_VALUE;
        }
        try {
            dbFileIterator.open();
            while (dbFileIterator.hasNext()) {
                this.numOfTuples++;
                Tuple tuple = dbFileIterator.next();
                pageid.add(tuple.getRecordId().getPageId());
                //第一遍遍历寻找最大最小值
                for (int i = 0; i < numOfFields; i++) {
                    if (tuple.getField(i).getType().equals(Type.INT_TYPE)) {
                        int fieldValue = Integer.parseInt(tuple.getField(i).toString());
                        if (min[i] > fieldValue) min[i] = fieldValue;
                        if (max[i] < fieldValue) max[i] = fieldValue;
                    }
                }
            }
            dbFileIterator.rewind();

            //第二遍新建直方图对象
            for(int i=0;i<numOfFields;i++){
                if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE))histogram[i]=new IntHistogram(NUM_HIST_BINS,min[i],max[i]);
                else histogram[i]=new StringHistogram(NUM_HIST_BINS);
            }
            //第三遍添加数据
            while (dbFileIterator.hasNext()){
                Tuple tuple=dbFileIterator.next();
                for (int i = 0; i <numOfFields ; i++) {
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE))
                        ((IntHistogram)histogram[i]).addValue(Integer.parseInt(tuple.getField(i).toString()));
                    else ((StringHistogram)histogram[i]).addValue(tuple.getField(i).toString());
                }
            }
            dbFileIterator.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        this.numOfPages=pageid.size();*/

        //依然不对
        this.ioCostPerPage = ioCostPerPage;
        statsMap.put(Database.getCatalog().getTableName(tableid),this);
        file = Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = file.getTupleDesc();
        histogram = new Object[tupleDesc.numFields()];//柱状图数组
        Set<PageId> pageIdSet = new HashSet<>();//用来计算这个文件有多少个page
        //遍历该表，求得每int型列的最大最小值和page个数,和tuple的个数
        int[][] minMax = new int[tupleDesc.numFields()][2];
        for (int i = 0; i < minMax.length; i++) {
            minMax[i][0] = Integer.MAX_VALUE;
            minMax[i][1] = Integer.MIN_VALUE;
        }
        iterator = file.iterator(new TransactionId());
        try {
            iterator.open();
            while (iterator.hasNext()){
                Tuple next = iterator.next();
                ++numOfTuples;
                pageIdSet.add(next.getRecordId().getPageId());
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        IntField field = (IntField)next.getField(i);
                        minMax[i][0] = Math.min(minMax[i][0],field.getValue());
                        minMax[i][1] = Math.max(minMax[i][1],field.getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }finally {
            iterator.close();
        }
        numOfPages = pageIdSet.size();


        //创建每列对应的直方图
        for (int i = 0; i < histogram.length; i++) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                histogram[i] = new IntHistogram(NUM_HIST_BINS,minMax[i][0],minMax[i][1]);
            }else histogram[i] = new StringHistogram(NUM_HIST_BINS);
        }

        //再次遍历，插入每个元组
        try {
            iterator.open();
            while (iterator.hasNext()){
                Tuple next = iterator.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        ((IntHistogram)histogram[i]).addValue(((IntField)next.getField(i)).getValue());
                    }else {
                        ((StringHistogram)histogram[i]).addValue(((StringField)next.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }finally {
            iterator.close();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //页数要自己算，因为有可能传进来的是btreefile
        //每个tuple都有自己对应的pageid,计算数量
        return ioCostPerPage*numOfPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor*numOfTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here

        if(file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE))return ((IntHistogram)histogram[field]).estimateSelectivity(op,Integer.parseInt(constant.toString()));
        else return ((StringHistogram)histogram[field]).estimateSelectivity(op,constant.toString());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here

        return numOfTuples;
    }

}
