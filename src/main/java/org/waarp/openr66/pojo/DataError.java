package org.waarp.openr66.pojo;

import java.util.ArrayList;
import java.util.HashMap;

public class DataError {

    private HashMap<String, ArrayList<String>> details;

    public DataError() {
        details = new HashMap<String, ArrayList<String>>();
    }

    public boolean isError() {
        return details.size() > 0;
    }

    public void add(String key, String message) {
        if (details.containsKey(key)) {
            details.get(key).add(message);
        } else {
            ArrayList<String> list = new ArrayList<String>();
            list.add(message);
            details.put(key, list);
        }
    }

    public HashMap<String, ArrayList<String>> getDetails() {
        return details;
    }
} 
