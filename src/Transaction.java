package com.anomalies;

import java.util.GregorianCalendar;

public class Transaction implements Comparable<Transaction>{

    private final GregorianCalendar timestamp;
    private final double amount;
    private final int counter;

    public Transaction(GregorianCalendar date, double value, int seq){
        timestamp = date;
        amount = value;
        counter = seq;
    }

    public static Transaction generateTransaction(String date, String value, int counter){
        String daystring = date.substring(0,10);
        String time = date.substring(11);
        String[] split1 = daystring.split("-");
        String[] split2 = time.split(":");

        Integer year = Integer.parseInt(split1[0]);
        Integer month = Integer.parseInt(split1[1]) - 1;
        Integer day = Integer.parseInt(split1[2]);
        Integer hour = Integer.parseInt(split2[0]);
        Integer minute = Integer.parseInt(split2[1]);
        Integer second = Integer.parseInt(split2[2]);

        return new Transaction(new GregorianCalendar(year, month, day, hour, minute, second), Double.parseDouble(value), counter);
    }

    public static Transaction generateTransaction(String date, String value){
        return generateTransaction(date, value, 0);
    }

    public GregorianCalendar getDate(){
        return timestamp;
    }

    public double getAmount(){
        return amount;
    }

    public int getCounter(){ return counter; }

    @Override
    public int compareTo(Transaction o){
        if((this.timestamp).compareTo(o.getDate()) == 0)
            return ((Integer) this.counter).compareTo(o.getCounter());
        else
            return (this.timestamp).compareTo(o.getDate());
    }
}