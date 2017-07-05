package com.anomalies;

import java.util.Iterator;
import java.lang.Comparable;
import com.anomalies.Transaction;

public class HeapObject implements Comparable<HeapObject>{
    public Iterator<Transaction> myIter;
    public Transaction myMostRecentTxn;

    public HeapObject(Transaction txn, Iterator<Transaction> iter){
        this.myIter = iter;
        this.myMostRecentTxn = txn;
    }

    @Override
    public int compareTo(HeapObject o){
        return (this.myMostRecentTxn).compareTo(o.myMostRecentTxn);
    }
}