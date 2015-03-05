/**
 * ROMA client binding for YCSB.
 *
 * All YCSB records are mapped to a ROMA *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.rakuten.rit.roma.romac4j.RomaClient;
//import com.rakuten.rit.roma.romac4j.connection.*;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.io.*;
import org.apache.log4j.Logger;

import java.math.BigInteger;

public class RomaClients extends DB {



    private static RomaWrapper rw = null;
    private static RomaClient rc = null;

    public void init() throws DBException {
        try {
            java.util.Properties props = getProperties();
            int port;

            String portString = props.getProperty("roma.port");

            if (portString != null) {
                port = Integer.parseInt(portString);
            } else {
                port = 10011;
            }
            String host = props.getProperty("roma.host");
            //RomaSocketPool.init();
            if (rw == null) {
                rw = RomaWrapper.getInstance();
            }
            //rc = rw.getConnection("localhost_10001");
            rc = rw.getConnection(host + "_" + port);
        } catch (Exception e) {
        }
    }

    public void cleanup() throws DBException {
        if (rc != null)
            rc.destroy();
    }


    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
    try {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(rc.get(key));
        ObjectInputStream in = new ObjectInputStream(byteIn);
        
        HashMap<String, byte[]> values = new HashMap<String, byte[]>();
        values = (HashMap<String, byte[]>) in.readObject();
        byteIn.close();
        in.close();
        if (values == null) return -2;
        if (values.keySet().isEmpty()) return -2;
        if (fields == null) fields = values.keySet();

        for (String k: fields) {
            byte[] v = values.get(k);
            if (v == null) return -2;
            result.put(k, new ByteArrayByteIterator(v));
        }
        return 0;
    } catch (Exception e) {
        return -1;
    }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
        HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();

        for (String k: values.keySet()) {
          new_values.put(k, values.get(k).toArray());
        }
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(new_values);
        rc.set(key, byteOut.toByteArray(), 0);     
        byteOut.close();
        out.close();
        return 0;
    } catch (Exception e) {
        return -1;
    }
    }

    @Override
    public int delete(String table, String key) {
    try {
         return rc.delete(key) ? 0 : 1;
    } catch (Exception e) {
         return -1;
    }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
        HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();

        for (String k: values.keySet()) {
          new_values.put(k, values.get(k).toArray());
        }
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(new_values);
        rc.set(key, byteOut.toByteArray(), 0);
        byteOut.close();
        out.close();
        return 0;
    } catch (Exception e) {
        return -1;
    }
    }


    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // This method is not supported inside ROMA
        return -1;
    }

}
