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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //next   hasnext   add
        return new Iterator<TDItem>() {
            private int cursor=0;
            @Override
            public boolean hasNext() {
                if((cursor)<numFields())return true;
                return false;
            }

            @Override
            public TDItem next() {
                //return new TDItem(getFieldType(cursor),getFieldName(cursor));
                if(!hasNext())return null;
                TDItem tdItem;
                if(fieldAr == null||cursor>fieldAr.length-1)tdItem=new TDItem(typeAr[cursor],"");
                else tdItem=new TDItem(typeAr[cursor],fieldAr[cursor]);
                cursor++;
                return tdItem;
            }
        };
        //return null;
    }

    private static final long serialVersionUID = 1L;

    private Type[] typeAr;
    private String[] fieldAr;

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
        this.typeAr=typeAr;
        this.fieldAr=fieldAr;
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
        this.typeAr=typeAr;
    }

    /**
     * @return the number of fields in this TupleDesc        元素数量
     */
    public int numFields() {
        // some code goes here
        return typeAr.length;
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
        if(fieldAr==null || i>fieldAr.length)return null;
        return fieldAr[i];
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
        //return null;
        return typeAr[i];
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
        if(name==null)throw new NoSuchElementException();
        if(fieldAr==null)throw new NoSuchElementException();
        for(int i=0;i<fieldAr.length;i++){
            if(fieldAr[i]!=null && fieldAr[i].equals(name))return i;
        }
        throw new NoSuchElementException();
        }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     *         字节大小
     */
    public int getSize() {
        // some code goes here
        //return 0;
        int size=0;
        for(int i=0;i<typeAr.length;i++){
            size+=typeAr[i].getLen();
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
        // some code goes here
        //return null;
        int len1=td1.numFields();
        int len2=td2.numFields();
//        if(len1==0 && len2==0)return null;
//        if(len1==0)return td2;
//        if(len2==0)return td1;
        int index=0;
        Type[] td3Type=new Type[len1+len2];
        String[] td3Field=new String[len1+len2];
//        for(index=0;index<len1;index++){
//            td3Type[index]=td1.typeAr[index];
//            String tmp;
//            System.out.println(td1.fieldAr[index]);
//            if((tmp=td1.fieldAr[index])!=null) {System.out.println("not null");td3Field[index]=tmp;}
//            else td3Field[index]=null;
//        }
//        for(;index<len1+len2;index++){
//            td3Type[index]=td2.typeAr[index-len1];
//            String tmp;
//            if((tmp=td2.fieldAr[index-len1])!=null) td3Field[index]=tmp;
//            //td3Field[index]=td2.fieldAr[index-len1];
//        }
        Iterator<TDItem> iterator1=td1.iterator();
        Iterator<TDItem> iterator2=td2.iterator();
        while (iterator1.hasNext()){
            TDItem tdItem=iterator1.next();
            td3Type[index]=tdItem.fieldType;
            td3Field[index++]=tdItem.fieldName;
        }
        while (iterator2.hasNext()){
            TDItem tdItem=iterator2.next();
            td3Type[index]=tdItem.fieldType;
            td3Field[index++]=tdItem.fieldName;
        }
        return new TupleDesc(td3Type,td3Field);
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
        if(o==null)return false;
        if (o instanceof TupleDesc) {
            TupleDesc otd=(TupleDesc)o;
            if(this.typeAr==null && otd.typeAr==null)return true;
            if(otd.typeAr==null)return false;
            if(typeAr.length!=otd.typeAr.length)return false;
            if(Arrays.equals(this.typeAr,otd.typeAr))return true;
//            for(int i=0;i<typeAr.length;i++){
//                if(typeAr[i]!=otd.typeAr[i])return false;
//            }
//            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        String s="";
        for(int i=0;i<typeAr.length;i++){
            if(fieldAr==null || i>fieldAr.length-1)s+=typeAr[i];
            else s+=typeAr[i]+"("+fieldAr[i]+")";
            if(i<typeAr.length-1)s+=",";
        }
        return s;
    }
}
