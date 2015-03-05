package com.yahoo.ycsb.db;

import com.rakuten.rit.roma.romac4j.RomaClient;

import com.yahoo.ycsb.DB;

public class RomaWrapper {
    private static RomaWrapper wrapper = new RomaWrapper();

    private RomaWrapper() {}

    public static RomaWrapper getInstance() {
        return wrapper;
    }

    private static RomaClient rc = null;

    public static synchronized RomaClient getConnection(String host) {
        if (rc == null) {
            rc = new RomaClient(host);
        }
        return rc;
    }
}
