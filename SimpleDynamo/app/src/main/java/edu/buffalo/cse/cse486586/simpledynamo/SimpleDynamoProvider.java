package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;

	private ArrayList<String> _knownPids;
    private ConcurrentHashMap<String,VersionPair> _localValues;
	private String _myPort;
    private HashMap<Integer,AtomicBoolean> _insertWait;
    private int _insertWaitCount;
	private HashMap<Integer,AtomicBoolean> _queryWait;
	private HashMap<Integer,AtomicBoolean> _queryWaitAll;
	private HashMap<Integer,VersionPair> _queryValue;
	private HashMap<Integer,ArrayList<VersionPair>> _queryValues;
    private int _queryValueCount;
    private int _queryValuesCount;
    private AtomicBoolean _isCaughtUp;


    public String predPid(String pid) {
        try {
            String myHash = genHash(pid);
            String maxSmaller = "\0";
            String res = lookupPid(lookupMyPort());
            for (String remotePid : _knownPids) {
                String remoteHash = genHash(remotePid);
                if (remoteHash.compareTo(myHash) < 0) { //if remoteHash is smaller than myHash
                    if (remoteHash.compareTo(maxSmaller) > 0) { //if remoteHash is bigger than previous maximum smaller than myHash
                        maxSmaller = remoteHash;
                        res = remotePid;
                    }

                }
            }
            if (maxSmaller.equals("\0")) {//meaning no nodes smaller. find overall max
                for (String remotePid : _knownPids) {
                    String remoteHash = genHash(remotePid);
                    if (remoteHash.compareTo(maxSmaller) > 0) {
                        maxSmaller = remoteHash;
                        res = remotePid;
                    }
                }
            }
            return res;
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    public String succPid(String pid) {
        try {
            String myHash = genHash(pid);
            String minBigger = "z";
            String res = lookupPid(lookupMyPort());
            for (String remotePid : _knownPids) {
                String remoteHash = genHash(remotePid);
                if (remoteHash.compareTo(myHash) > 0) { //if remoteHash is bigger than myHash
                    if (remoteHash.compareTo(minBigger) < 0) { //if remoteHash is smaller than previous minimum bigger than myHash
                        minBigger = remoteHash;
                        res = remotePid;
                    }

                }
            }
            if (minBigger.equals("z")) {//meaning no nodes larger. find overall min
                for (String remotePid : _knownPids) {
                    String remoteHash = genHash(remotePid);
                    if (remoteHash.compareTo(minBigger) < 0) {
                        minBigger = remoteHash;
                        res = remotePid;
                    }
                }
            }
            return res;
        } catch (NoSuchAlgorithmException e) {
            return "";
        }


    }

    public String lookupOwner(String key){
        for(String pid: _knownPids){
            if(belongsTo(key,pid)){
                return pid;
            }
        }
        return "NA";
    }

    public boolean belongsTo(String key, String pid){
        try {
            String prevNodeHash = genHash(predPid(pid));
            String nodeHash = genHash(pid);
            String dataHash = genHash(key);

            if(nodeHash.compareTo(dataHash) > 0 && dataHash.compareTo(prevNodeHash) > 0){
                return true;
            }else if((prevNodeHash.compareTo(nodeHash) > 0) && (dataHash.compareTo(prevNodeHash) > 0 || dataHash.compareTo(nodeHash) < 0)) {
                return true;
            }else if(prevNodeHash.equals(nodeHash)){
                return true;
            }
            return false;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean belongsToGroup(String key, String pid){
        String myPid = pid;
        String pred = predPid(myPid);
        String predPred = predPid(pred);
        return (belongsTo(key,myPid) || belongsTo(key,pred) || belongsTo(key,predPred));
    }

    public String lookupMyPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String port = String.valueOf((Integer.parseInt(portStr) * 2));
        return port;
    }

    public String lookupPid(String port) {
        if (port.equals(REMOTE_PORT0)) return "5554";
        if (port.equals(REMOTE_PORT1)) return "5556";
        if (port.equals(REMOTE_PORT2)) return "5558";
        if (port.equals(REMOTE_PORT3)) return "5560";
        return "5562";
    }

    public String lookupPort(String pid) {
        if (pid.equals("5554")) return REMOTE_PORT0;
        if (pid.equals("5556")) return REMOTE_PORT1;
        if (pid.equals("5558")) return REMOTE_PORT2;
        if (pid.equals("5560")) return REMOTE_PORT3;
        return REMOTE_PORT4;
    }

    public ArrayList<String> replicationGroup(String primaryPid) {
        ArrayList<String> res = new ArrayList<String>();
        String n1 = succPid(primaryPid);
        String n2 = succPid(n1);
        res.add(primaryPid);
        res.add(n1);
        res.add(n2);
        Log.v("replicationGroup", primaryPid + "," + n1 + ","+ n2);
        return res;
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.equals("*")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteAll");
        }else if(selection.equals("@")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteAllLocal");
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete"+"\0"+selection);
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        Log.v("In Charge Of Insert", key);
        int index = ++_insertWaitCount;
        _insertWait.put(index,new AtomicBoolean(true));

        //TODO: write to file for backup
        //writeToFile(key+".bck",value);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert" + "\0" + key+"\0"+value+"\0"+index);
        int count = 0;
        synchronized (_insertWait.get(index)) {
            while (_insertWait.get(index).get()) {
                try{
                    _insertWait.get(index).wait();
                }catch(Exception e){}
            }
        }
        return uri;
	}

	@Override
	public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        _myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.v("CREATE","CREATE");
        Log.v("myPort is ",""+_myPort);

         _insertWait = new HashMap<Integer, AtomicBoolean>();
        _insertWaitCount = 0;
         _queryWait = new HashMap<Integer, AtomicBoolean>();
        _queryWaitAll = new HashMap<Integer, AtomicBoolean>();
        _queryValue = new HashMap<Integer, VersionPair>();
        _queryValues = new HashMap<Integer, ArrayList<VersionPair>>();
        _queryValueCount = 0;
        _queryValuesCount = 0;
        _isCaughtUp = new AtomicBoolean(false);


        _knownPids = new ArrayList<String>();
        _knownPids.add(lookupPid(REMOTE_PORT0));
        _knownPids.add(lookupPid(REMOTE_PORT1));
        _knownPids.add(lookupPid(REMOTE_PORT2));
        _knownPids.add(lookupPid(REMOTE_PORT3));
        _knownPids.add(lookupPid(REMOTE_PORT4));
        _localValues = new ConcurrentHashMap<String, VersionPair>();


        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "catchUp");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //e.printStackTrace();
        }

        return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        Log.v("In Charge Of Query", selection);
		if (selection.equals("*")){
            int index = ++_queryValuesCount;
            _queryWaitAll.put(index,new AtomicBoolean(true));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryAll"+"\0"+index);
            synchronized (_queryWaitAll) {
                while (_queryWaitAll.get(index).get()) {
                    try {
                        _queryWaitAll.wait();
                    }catch(Exception e){}
                }
            }
            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);

            for(VersionPair v : _queryValues.get(index)) {
                if(null != v && !v._isDelete){
                    String columnValues[] = {v._key,v._value};
                    mc.addRow(columnValues);
                }
            }
            return mc;
        }else if(selection.equals("@")){
            Log.v("QueryLocal","Dump " + _localValues.keySet().size());

            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);
            for(VersionPair v: actuallyQueryAll()){
                if(null != v && !v._isDelete) {
                    String columnValues[] = {v._key, v._value};
                    mc.addRow(columnValues);
                }
            }

            return mc;
        }else{
            int index = ++_queryValueCount;
            _queryWait.put(index,new AtomicBoolean(true));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query"+"\0"+selection+"\0"+index);
            synchronized (_queryWait) {
                while (_queryWait.get(index).get()) {
                    try {
                        _queryWait.wait();
                    }catch(Exception e){}
                }
            }
            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);
            if(null != _queryValue.get(index)) {
                String columnValues[] = {_queryValue.get(index)._key, _queryValue.get(index)._value};
                mc.addRow(columnValues);
            }
            return mc;
        }
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void actuallyInsert(String key, String value) {
        try {
            VersionPair temp;
            if(_localValues.containsKey(key)){
                int oldVersion = _localValues.get(key)._version;
                temp = new VersionPair(key,value,oldVersion+1,false);
            }else{
                temp = new VersionPair(key,value,1,false);
            }
            _localValues.put(key,temp);

        } catch (Exception e) {
        }
    }

    public void actuallyDelete(String key) {
        synchronized (_isCaughtUp) {
            while (!_isCaughtUp.get()) {
                try{
                    _isCaughtUp.wait();
                }catch(Exception e){}
            }
        }
        if(_localValues.containsKey(key)){
            int oldVersion = _localValues.get(key)._version;
            VersionPair temp = new VersionPair(key,"",oldVersion+1,true);
            _localValues.put(key,temp);
        }
    }

    public void actuallyDeleteAll() {
        synchronized (_isCaughtUp) {
            while (!_isCaughtUp.get()) {
                try{
                    _isCaughtUp.wait();
                }catch(Exception e){}
            }
        }
        for(String k: _localValues.keySet()){
            int oldVersion = _localValues.get(k)._version;
            VersionPair temp = new VersionPair(k, "", oldVersion + 1, true);
            _localValues.put(k,temp);
        }
    }

    public void actuallyDeleteAllFrom(String pid) {
        synchronized (_isCaughtUp) {
            while (!_isCaughtUp.get()) {
                try{
                    _isCaughtUp.wait();
                }catch(Exception e){}
            }
        }
        for(String k: _localValues.keySet()){
            if(belongsTo(k,pid)) {
                int oldVersion = _localValues.get(k)._version;
                VersionPair temp = new VersionPair(k, "", oldVersion + 1, true);
                _localValues.put(k,temp);
            }
        }
    }

    public ArrayList<VersionPair> keyRequest(String pid) {
        ArrayList<VersionPair> res = new ArrayList<VersionPair>();
        synchronized (_isCaughtUp) {
            while (!_isCaughtUp.get()) {
                try{
                    _isCaughtUp.wait();
                }catch(Exception e){}
            }
        }
        for(String k: _localValues.keySet()){
            if(belongsToGroup(k,pid)){
                res.add(_localValues.get(k));
            }
        }
        return res;
    }

    public VersionPair actuallyQuery(String key) {
        try {
            synchronized (_isCaughtUp) {
                while (!_isCaughtUp.get()) {
                    try{
                        _isCaughtUp.wait();
                    }catch(Exception e){}
                }
            }
            VersionPair res = _localValues.get(key);
            return res;

        } catch (Exception e) {
        }
        return null;
    }

    public ArrayList<VersionPair> actuallyQueryAll() {
        synchronized (_isCaughtUp) {
            while (!_isCaughtUp.get()) {
                try{
                    _isCaughtUp.wait();
                }catch(Exception e){}
            }
        }
        ArrayList<VersionPair> res = new ArrayList<VersionPair>();
        for(String k: _localValues.keySet()){
            res.add(_localValues.get(k));
        }
        return res;
    }

    private Object readFromSocket(Socket socket){

        /*try {
            InputStream is = socket.getInputStream();
            byte[] messageRecieved = new byte[10000];
            int messageLength = is.read(messageRecieved, 0, 10000);
            String temp = "";
            for (int i = 0; i < messageLength; i++) {
                temp += (char) messageRecieved[i];
            }
            return temp;
        }catch (IOException e){

        }*/
        try {
            ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
            Object temp =  is.readObject();
            return temp;

        }catch(IOException e){

        }catch(ClassNotFoundException e){

        }



        return null;
    }

    private void writeToSocket(Socket socket,Object message){
        /*try{
            OutputStream os = socket.getOutputStream();
            byte[] ba = (message).getBytes();
            os.write(ba);
        }catch(IOException e){

        }*/

        try {
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            os.flush();
            os.writeObject(message);
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private ArrayList<String> backupInserts(){
        //http://stackoverflow.com/questions/2102952/listing-files-in-a-directory-matching-a-pattern-in-java
        File dir = getContext().getFilesDir();
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.endsWith(".bck");
            }
        });
        ArrayList<String> res = new ArrayList<String>();
        for(File f: files){
            res.add(f.getName());
        }
        return res;
    }



    //*********************
    //*****SERVER TASK*****
    //*********************

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    String temp = (String)readFromSocket(socket);
                    String splits[] = temp.split("\0");
                    String operation = splits[0];

                    if (operation.equals("insert")) {
                        actuallyInsert(splits[1],splits[2]);
                        writeToSocket(socket,"ACK");
                    }else if(operation.equals("query")) {
                        VersionPair res = actuallyQuery(splits[1]);
                        writeToSocket(socket,res);
                    }else if(operation.equals("queryAll")){
                        ArrayList<VersionPair> res = actuallyQueryAll();
                        writeToSocket(socket,res);
                    }else if(operation.equals("delete")) {
                        actuallyDelete(splits[1]);
                        writeToSocket(socket,"ACK");
                    }else if(operation.equals("deleteAll")){
                        actuallyDeleteAll();
                        writeToSocket(socket,"ACK");
                    }else if(operation.equals("deleteAllFrom")){
                        actuallyDeleteAllFrom(splits[1]);
                        writeToSocket(socket,"ACK");
                    }if(operation.equals("keyRequest")){
                        ArrayList<VersionPair> res  = keyRequest(splits[1]);
                        writeToSocket(socket,res);
                        readFromSocket(socket);
                    }

                }
            }catch(IOException e){
                return null;
            }

        }


    }

    //*********************
    //*****CLIENT TASK*****
    //*********************

    private class ClientTask extends AsyncTask<String, Void, Void> {


        private void handleInsert(String key,String value,String index){
            String primaryPid = lookupOwner(key);


            int failCount;
            while(true) {
                failCount = 0;
                for (String pid : replicationGroup(primaryPid)) { //for now send to all
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(lookupPort(pid)));
                        socket.setSoTimeout(1000);
                        writeToSocket(socket, "insert" + "\0" + key + "\0" + value);
                        String temp = (String) readFromSocket(socket);
                        if (null != temp) {
                            Log.v("Insert Success", key + "," + pid);
                        } else {
                            Log.v("Insert Fail", key + "," + pid);
                            failCount += 1;
                        }
                    } catch (SocketTimeoutException e) {
                        //Log.v("Insert Failed2",lookupPid(lookupMyPort()));
                    } catch (IOException e) {
                        //Log.v("Insert Failed3",lookupPid(lookupMyPort()));
                    }
                }
                if (failCount <= 1){
                    break;
                }
            }

            synchronized (_insertWait.get(Integer.parseInt(index))){
                _insertWait.get(Integer.parseInt(index)).set(false);
                _insertWait.get(Integer.parseInt(index)).notify();
            }


            //TODO: Delete file for that key
        }

        private void handleQuery(String key,String index){
            String primaryPid = lookupOwner(key);
            VersionPair latestVersionPair = null;
            //ask all in preference list, choose highest version
            Log.v("Querying",key);
            int failCount;
            while(true) {
                failCount = 0;
                for (String pid : replicationGroup(primaryPid)) {

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(lookupPort(pid)));
                        socket.setSoTimeout(1000);
                        writeToSocket(socket, "query" + "\0" + key);
                        VersionPair temp = (VersionPair) readFromSocket(socket);
                        latestVersionPair = VersionPair.latest(latestVersionPair, temp);
                        if (null != temp) {
                            Log.v("Insert Success", key + "," + pid);
                        } else {
                            Log.v("Insert Fail", key + "," + pid);
                            failCount += 1;
                        }
                    } catch (SocketTimeoutException e) {
                    } catch (IOException e) {
                    }
                }
                if(failCount <= 1){
                    break;
                }
            }
            synchronized (_queryWait){
                _queryValue.put(Integer.parseInt(index), latestVersionPair);
                _queryWait.get(Integer.parseInt(index)).set(false);
                _queryWait.notify();
            }

        }

        private void handleQueryAll(String index){

            ArrayList<VersionPair> all = new ArrayList<VersionPair>();

            int failCount;
            while(true) {
                failCount = 0;
                for (String pid : _knownPids) {
                    try {
                        String primaryPort = lookupPort(pid);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(primaryPort));
                        socket.setSoTimeout(1000);
                        writeToSocket(socket, "queryAll");
                        ArrayList<VersionPair> temp = (ArrayList<VersionPair>) readFromSocket(socket);
                        if (null != temp) {
                            for (VersionPair v : temp) {
                                all.add(v);
                            }
                        }else{
                            failCount += 1;
                        }
                    } catch (SocketTimeoutException e) {
                    } catch (IOException e) {
                    }
                }
                if (failCount <= 1){
                    break;
                }
            }
            synchronized (_queryWaitAll){
                _queryValues.put(Integer.parseInt(index),VersionPair.latest(all));
                _queryWaitAll.get(Integer.parseInt(index)).set(false);
                _queryWaitAll.notify();
            }


        }

        private void handleDelete(String key){
            String primaryPid = lookupOwner(key);

            for(String pid: replicationGroup(primaryPid)) { //for now send to all
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lookupPort(pid)));
                    socket.setSoTimeout(1000);
                    writeToSocket(socket,"delete" + "\0" + key);
                    String temp = (String)readFromSocket(socket);
                    //CHECK for ACK
                } catch (SocketTimeoutException e) {
                } catch (IOException e) {
                }
            }
        }

        private void handleDeleteAll(){

            for(String pid: _knownPids){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lookupPort(pid)));
                    socket.setSoTimeout(1000);
                    writeToSocket(socket,"deleteAll");
                    String temp = (String)readFromSocket(socket);

                } catch(SocketTimeoutException e){
                } catch (IOException e) {
                }
            }

        }

        private void handleDeleteLocal(){

            for(String pid: replicationGroup(lookupPid(lookupMyPort()))) { //for now send to all
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lookupPort(pid)));
                    socket.setSoTimeout(1000);
                    writeToSocket(socket,"deleteAllFrom"+"\0"+lookupPid(lookupMyPort()));
                    String temp = (String)readFromSocket(socket);

                } catch(SocketTimeoutException e){
                } catch (IOException e) {
                }
            }

        }

        private void handleCatchUp(){

            String myPid = lookupPid(lookupMyPort());
            ArrayList<VersionPair> all = new ArrayList<VersionPair>();


            for(String pid: _knownPids) {
                if (!myPid.equals(pid)) {
                    try {
                        Log.v("Catch up request", pid);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(lookupPort(pid)));
                        writeToSocket(socket, "keyRequest" + "\0" + myPid);
                        ArrayList<VersionPair> temp = (ArrayList<VersionPair>) readFromSocket(socket);
                        writeToSocket(socket, "ACK");
                        for (VersionPair v : temp) {
                            all.add(v);
                            //Log.v("Learned from", v._key + "," + pid);
                        }
                    } catch (Exception e) {
                        Log.v("Catch up Exception", "" + e);
                    }
                }

            }


            Log.v("Values Before",""+_localValues.keySet().size());
            for(VersionPair v: VersionPair.latest(all)){
                _localValues.put(v._key,v);
                Log.v("Learned",v._key+","+v._value);
            }
            Log.v("Values After",""+_localValues.keySet().size());
            synchronized (_isCaughtUp){
                _isCaughtUp.set(true);
                _isCaughtUp.notifyAll();
            }




        }

        @Override
        protected Void doInBackground(String... msgs) {
            String[] splits = msgs[0].split("\0");;

            String operation = splits[0];

            if(operation.equals("insert")){
                Log.v("Handling insert:",splits[1]);
                handleInsert(splits[1],splits[2],splits[3]);
            }else if(operation.equals("query")){
                handleQuery(splits[1],splits[2]);
            }else if(operation.equals("queryAll")){
                handleQueryAll(splits[1]);
            }else if(operation.equals("delete")){
                handleDelete(splits[1]);
            }else if(operation.equals("deleteAll")){
                handleDeleteAll();
            }else if(operation.equals("deleteAllLocal")){
                handleDeleteLocal();
            }else if(operation.equals("catchUp")){
                handleCatchUp();
            }

            return null;
        }
    }
}
