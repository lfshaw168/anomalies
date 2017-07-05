package com.anomalies;

import java.util.*;
import java.io.*;
import java.text.*;
import java.math.*;
import java.lang.System;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main{

    // global variables
    private static int STDEV_MULTIPLE = 3;

    // input: path to batch file (String), path to stream file (String)
    public static void main(String[] args){
        String batch = "", stream = "", target = "";

        // extract input arguments
        try {
            batch = args[0];
            stream = args[1];
            target = args[2];
        }
        catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Not enough input arguments.");
        }
        if(batch.length() == 0 || stream.length() == 0 || target.length() == 0)
            throw new IllegalArgumentException();

        Object[] parameters = constructNetwork(batch);
        Integer D = (Integer) parameters[0];
        Integer T = (Integer) parameters[1];
        Map<String,User> roster = (Map<String,User>) parameters[2];
        Map<User,Set<User>> adjacencyList = (Map<User,Set<User>>) parameters[3];

        BufferedWriter writer = null;

        try{
            File file = new File(target);

            // overwrite the output file
            file.createNewFile();

            writer = new BufferedWriter(new FileWriter(file));
            beginStreamingData(writer, stream, roster, adjacencyList, D, T);

        } catch(IOException e){
            e.printStackTrace();
        }
        finally{
            try{
                if(writer != null)
                    writer.close();
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    // function: extracts out the D and T values and contructs a graph of Users whose edges are kept as an adjacency
    //           list (HashSets keyed by Users within a HashMap)
    // input: path to batch file (String)
    // output: an Object[] of 4 Objects: (1) the degree "D" (int); (2) the averaging value "T" (int);
    //                                   (3) a "roster" HashMap of Users keyed by their ID (String); and
    //                                   (4) a "network" HashMap containing the adjacency list of the User graph:
    //                                       a HashSet of friend Users are keyed by User for each User
    //
    // (assumes the batch file contains a JSON object with D and T as its first line)
    private static Object[] constructNetwork(String batchPath){
        Object[] returnValues = new Object[4];
        Map<String,User> roster = new HashMap<String,User>();
        Map<User,Set<User>> network = new HashMap<User,Set<User>>();

        JSONParser parser = new JSONParser();
        BufferedReader reader = null;

        try{
            reader = new BufferedReader(new FileReader(new File(batchPath)));
            String currObj;

            // extracts the first JSON object, which is assumed to correspond to the D and T values
            if((currObj = reader.readLine()) != null){
                JSONObject initializer = (JSONObject) parser.parse(currObj);
                returnValues[0] = Integer.parseInt((String) initializer.get("D"));
                returnValues[1] = Integer.parseInt((String) initializer.get("T"));
            }
            returnValues[2] = roster;
            returnValues[3] = network;

            String timestamp = null;
            int counter = 0;

            // iterates through each line of the input batch file
            while ((currObj = reader.readLine()) != null){
                JSONObject item = (JSONObject) parser.parse(currObj);
                String currEvent = (String) item.get("event_type");

                if(timestamp != null){
                    if(timestamp != (String) item.get("timestamp")){
                        timestamp = (String) item.get("timestamp");
                        counter = 0;
                    }
                    else
                        counter++;
                } else
                    timestamp = (String) item.get("timestamp");

                switch(currEvent){
                    case "purchase":
                        addPurchase(roster, network, timestamp, (String) item.get("id"), (String) item.get("amount"), counter);
                        break;
                    case "befriend":
                        addFriend(roster, network, (String) item.get("id1"), (String) item.get("id2"));
                        break;
                    case "unfriend":
                        removeFriend(roster, network, (String) item.get("id1"), (String) item.get("id2"));
                        break;
                }
            }
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        } catch(ParseException e){
            e.printStackTrace();
        } finally{
            try{
                if(reader != null)
                    reader.close();
            } catch(Exception e){
                e.printStackTrace();
            }
        }

        return returnValues;
    }

    // function: goes into the roster (HashMap), checks if a User exists, and returns it;
    //           if it does not exist, the User is created, input into the roster and the network, and returned
    // input: roster (Map<String,User>) and network (Map<User,Set<User>>) from constructNetwork(), User id (String)
    // output: corresponding User
    private static User getUser(Map<String,User> roster, Map<User,Set<User>> network, String id){
        if(roster.get(id) == null){
            User newUser = new User(id);
            roster.put(id, newUser);
            network.put(newUser, new HashSet<User>());
            return newUser;
        }
        else
            return roster.get(id);
    }

    // function: adds a purchase into the corresponding User's ledger
    // input: roster (Map<String,User>) and network (Map<User,Set<User>>) from constructNetwork(), user id (String),
    //        transaction timestamp (String), User id (String), transaction amount (String)
    // output: none
    private static void addPurchase(Map<String,User> roster, Map<User,Set<User>> network, String timestamp, String id, String amount, int counter){
        User currUser = getUser(roster, network, id);
        currUser.addTransaction(timestamp, amount, counter);
    }

    // function: connects two Users in the network graph (HashMap) as "friends"; both User's adjacency lists are updated
    // intput: roster (Map<String,User>) and network (Map<User,Set<User>>) from constructNetwork(), first User's id (String)
    //         second User's id (String)
    // output: none
    private static void addFriend(Map<String,User> roster, Map<User,Set<User>> network, String id1, String id2){
        User user1 = getUser(roster, network, id1),
                user2 = getUser(roster, network, id2);
        Set<User> list1 = network.get(user1),
                list2 = network.get(user2);

        list1.add(user2);
        list2.add(user1);
    }

    // function: disconnects two Users in the network graph (HashMap) as "friends"; both User's adjacency lists are updated
    // input: roster (Map<String,User>) and network (Map<User,Set<User>>) from constructNetwork(), first User's id (String),
    //        second User's id (String)
    // output: none
    private static void removeFriend(Map<String,User> roster, Map<User,Set<User>> network, String id1, String id2){
        User user1 = getUser(roster, network, id1),
                user2 = getUser(roster, network, id2);
        Set<User> list1 = network.get(user1),
                list2 = network.get(user2);

        list1.remove(user2);
        list2.remove(user1);
    }

    // function: reads through the data stream and processes
    // input: BufferedWriter for the target log, path to the input stream log (String), roster (Map<String,User>) and
    //        network (Map<User,Set<User>>) from constructNetwork(), degree D, and averaging number T
    // output: none
    private static void beginStreamingData(BufferedWriter writer, String streamPath, Map<String,User> roster, Map<User,Set<User>> network, int D, int T){
        JSONParser parser = new JSONParser();
        BufferedReader reader = null;

        try{
            reader = new BufferedReader(new FileReader(streamPath));
            String currObj;

            // read through each line
            while ((currObj = reader.readLine()) != null){
                JSONObject item = (JSONObject) parser.parse(currObj);
                String currEvent = (String) item.get("event_type"),
                        timestamp = (String) item.get("timestamp");

                switch(currEvent){
                    case "purchase":
                        User currUser = roster.get((String) item.get("id"));

                        currUser.addTransaction(timestamp, (String) item.get("amount"));
                        checkIfAnomalous(writer, currUser, getNdegreeFriends(network, currUser, D), currUser.getLastTransaction(), T);
                        break;
                    case "befriend":
                        addFriend(roster, network, (String) item.get("id1"), (String) item.get("id2"));
                        break;
                    case "unfriend":
                        removeFriend(roster, network, (String) item.get("id1"), (String) item.get("id2"));
                        break;
                }
            }
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        } catch (ParseException e){
            e.printStackTrace();
        } finally{
            try{
                if(reader != null)
                    reader.close();
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    // function: creates the JSON String for a flagged anomalous transaction
    // input: User whose transaction is anomalous, anomalous transaction, mean and standard deviation for the D and T specified
    //        in beginStreamingData()
    // output: String for logging
    private static String assembleAnomalousOutputLine(User primeUser, Transaction currTxn, double mean, double stdev){
        DecimalFormat moneyFormat = new DecimalFormat("#.00"),
                       dateFormat = new DecimalFormat("00");
        moneyFormat.setRoundingMode(RoundingMode.FLOOR); // truncate, don't round

        Calendar txnTime = currTxn.getDate();
        int year = txnTime.get(Calendar.YEAR);
        int month = txnTime.get(Calendar.MONTH) + 1;
        int day = txnTime.get(Calendar.DATE);
        int hour = txnTime.get(Calendar.HOUR_OF_DAY);
        int minute = txnTime.get(Calendar.MINUTE);
        int second = txnTime.get(Calendar.SECOND);
        String timeStr = "" + year + "-" + dateFormat.format(month) + "-" + dateFormat.format(day) + " " +
                dateFormat.format(hour) + ":" + dateFormat.format(minute) + ":" + dateFormat.format(second);

        return "{\"event_type\":\"purchase\", \"timestamp\":\"" + timeStr + "\", \"id\":\"" + primeUser.getName() + "\", \"amount\":\"" +
                currTxn.getAmount() + "\", \"mean\":\"" + moneyFormat.format(mean) + "\", \"sd\":\"" + moneyFormat.format(stdev) + "\"}" +
                System.getProperty("line.separator");
    }

    // function: gets all Users within D degrees of the primeUser (assumes degreeD is >= 1)
    // input: the network's adjacency list as produced by constructNetwork(), User of interest, degree D
    // output: HashSet of all Users within D degrees of the primeUser
    private static Set<User> getNdegreeFriends(Map<User,Set<User>> adjacencyList, User primeUser, int D){
        Set<User> NthFriends = new HashSet<User>();
        Queue<GraphObject> currFriendList = new LinkedList<GraphObject>();
        currFriendList.offer(new GraphObject(primeUser, 0));

        // do a breadth-first traversal, inputting each user within a degree into the HashSet to be returned
        while(! currFriendList.isEmpty()){
            GraphObject currUser = currFriendList.poll();

            for(User i : adjacencyList.get(currUser.myUser)){
                if(! NthFriends.contains(i) && i != primeUser){
                    NthFriends.add(i);

                    if(currUser.degree + 1 < D)
                        currFriendList.add(new GraphObject(i, currUser.degree + 1));
                }
            }
        }
        return NthFriends;
    }

    // function: finds the most recent T transactions in primeUser's set of friends (who are each within D degrees) to
    //           which the current transaction is compared
    // input: the main writer to the anomalous log file, User of interest, User's friends within D degrees,
    //        Transaction of interest, the averaging value T
    // output: none
    private static void checkIfAnomalous(BufferedWriter writer, User primeUser, Set<User> friends, Transaction currTxn, int T){

        List<Double> meanList = new ArrayList<Double>(T);
        Comparator<HeapObject> comparator = new TxnComparator();
        int counter = 0;

        // use a heap containing HeapObjects, which are 2-tuples with (1) the timestamp to be compared and (2) the iterator
        // over the ledgers of each User; the former is used for heap comparisons
        PriorityQueue<HeapObject> myHeap = new PriorityQueue<HeapObject>(friends.size(), comparator);

        // input the iterators that traverse in reverse-chronological order with the most recent transaction
        for(User friend : friends){
            Iterator<Transaction> ledgerIter = friend.getReverseIterator();

            if(ledgerIter.hasNext())
                myHeap.offer(new HeapObject(ledgerIter.next(), ledgerIter));
        }

        // extract the most recent transaction amongst all friend Users and reinsert the iterator with that User's
        // next most recent transaction
        for(int i = 0; i < T && ! myHeap.isEmpty(); i++){
            HeapObject max = myHeap.poll();
            meanList.add(max.myMostRecentTxn.getAmount());

            if(max.myIter.hasNext()){
                HeapObject newItem = new HeapObject(max.myIter.next(), max.myIter);
                myHeap.offer(newItem);
                counter++;
            }
        }

        // if there are less than 2 purchases in the network, we exit
        if(counter < 2)
            return;

        double[] stats = getStatistics(meanList); // get the mean and stdev of the final list

        // if the transaction is anomalous, process it
        if(currTxn.getAmount() > stats[0] + STDEV_MULTIPLE*stats[1]){
            try{
                writer.write(assembleAnomalousOutputLine(primeUser, currTxn, stats[0], stats[1]));
            }catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    // function: get the arithmetic mean and standard deviation of a List of Doubles (input List<Double> contains at least one entry)
    // input: List of Doubles
    // output: double[] containing (1) the mean and (2) the standard deviation
    private static double[] getStatistics(List<Double> myList){
        double[] returnArray = new double[2];

        // arithmetic mean
        double mean = 0;
        for(Double i : myList)
            mean += i;
        returnArray[0] = mean/myList.size();

        // standard deviation
        double stdev = 0;
        for(Double i : myList)
            stdev += Math.pow(i-returnArray[0], 2);
        returnArray[1] = Math.sqrt(stdev/myList.size());

        return returnArray;
    }

}
