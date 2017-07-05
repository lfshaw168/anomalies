package com.anomalies;

import java.util.Comparator;

public class TxnComparator implements Comparator<HeapObject>
{
    @Override
    public int compare(HeapObject x, HeapObject y)
    {
        return -1 * x.compareTo(y);
    }
}