package com.anomalies;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import com.anomalies.Transaction;

public class User{

    private final String name;
    private Deque<Transaction> ledger;

    public User(){
        this("NO_NAME");
    }

    public User(String id){
        name = id;
        ledger = new LinkedList<Transaction>();
    }

    public String getName(){ return name; }

    public boolean addTransaction(String timestamp, String amount){
        return this.addTransaction(Transaction.generateTransaction(timestamp, amount));
    }

    public boolean addTransaction(String timestamp, String amount, int counter){
        return this.addTransaction(Transaction.generateTransaction(timestamp, amount, counter));
    }

    public boolean addTransaction(Transaction item){
        return ledger.offerLast(item);
    }

    public Transaction getLastTransaction(){ return ledger.peekLast(); }

    public Iterator<Transaction> getReverseIterator(){
        return ledger.descendingIterator();
    }

}