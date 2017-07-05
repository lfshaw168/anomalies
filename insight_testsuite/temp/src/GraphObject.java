package com.anomalies;

import com.anomalies.User;

public class GraphObject{
    public User myUser;
    public int degree;

    public GraphObject(User myUser, int degree){
        this.myUser = myUser;
        this.degree = degree;
    }
}