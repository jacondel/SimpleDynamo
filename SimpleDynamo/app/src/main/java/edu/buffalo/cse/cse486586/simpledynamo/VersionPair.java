package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jacob on 4/24/17.
 */
public class VersionPair implements Serializable {

    public String _key;
    public String  _value;
    public int _version;
    public boolean _isDelete;
    public VersionPair(String k, String v, int ver, boolean i){
        _key = k;
        _value = v;
        _version = ver;
        _isDelete = i;
    }


    public static VersionPair latest(VersionPair p1, VersionPair p2){
        if(null != p1 && null != p2) {
            if (p1._version > p2._version) {
                return p1;
            }
            return p2;
        }else if(null != p1){
            return p1;
        }else if(null != p2){
            return p2;
        }else{
            return null;
        }
    }

    public static ArrayList<VersionPair> latest(ArrayList<VersionPair> a){
        HashMap<String,VersionPair> h = new HashMap<String,VersionPair>();
        for(VersionPair p : a){
            if(h.containsKey(p._key) && h.get(p._key)._version < p._version){
                h.put(p._key,p);
            }else if(!h.containsKey(p._key)){
                h.put(p._key,p);
            }
        }
        ArrayList<VersionPair> res = new ArrayList<VersionPair>();
        for(String k: h.keySet()){
            res.add(h.get(k));
        }
        return res;
    }

}
