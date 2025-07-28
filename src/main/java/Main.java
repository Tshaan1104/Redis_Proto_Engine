import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    static int offset = 0;
    static boolean handshakeComplete = false;
    static boolean previousWrite = false;
    static ConcurrentHashMap<String, ArrayList<String>> listMap = new ConcurrentHashMap<>();
    static Object lock1 = new Object();
    static List<Socket> clientPriority = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        int port = 6379;

        try {
            if (args.length > 3 && args[2].equals("--replicaof")) {
                HashMap<String, String[]> map = new HashMap<>();
                serverSocket = new ServerSocket(Integer.parseInt(args[1]));
                serverSocket.setReuseAddress(true);
                String[] address = args[3].split(" ");
                String hostAddr = address[0];
                int portAddr = Integer.parseInt(address[1]);

                Socket masterSocket = new Socket(hostAddr, portAddr);
                BufferedReader masterInput = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                BufferedWriter masterOutput = new BufferedWriter(new OutputStreamWriter(masterSocket.getOutputStream()));

                masterOutput.write("*1\r\n$4\r\nPING\r\n");
                masterOutput.flush();
                String reply = masterInput.readLine();
                if (reply.equals("+PONG")) {
                    masterOutput.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + args[1] + "\r\n");
                    masterOutput.flush();
                    reply = masterInput.readLine();
                }
                if (reply.equals("+OK")) {
                    masterOutput.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
                    masterOutput.flush();
                    reply = masterInput.readLine();
                }
                if (reply.equals("+OK")) {
                    masterOutput.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
                    masterOutput.flush();
                    masterInput.readLine();
                }
                String curLine = masterInput.readLine();
                if (curLine.startsWith("$")) {
                    int rdbLength = Integer.parseInt(curLine.substring(1));
                    for (int i = 0; i < rdbLength; i++) {
                        masterInput.read();
                    }
                }
                Thread handleMaster = new Thread(() -> processClient(masterSocket, masterInput, map, new ArrayList<>(), args, "slave"));
                handleMaster.start();

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    Thread thread = new Thread(() -> processClient(clientSocket, br, map, new ArrayList<>(), args, "slave"));
                    thread.start();
                }
            } else {
                HashMap<String, String[]> map = new HashMap<>();
                if (args.length > 0 && args[0].equals("--port")) {
                    port = Integer.parseInt(args[1]);
                }
                serverSocket = new ServerSocket(port);
                serverSocket.setReuseAddress(true);
                ArrayList<OutputStream> replicaOutputStreams = new ArrayList<>();
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    Thread thread = new Thread(() -> processClient(clientSocket, clientInput, map, replicaOutputStreams, args, "master"));
                    thread.start();
                }
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    System.out.println("Error closing server socket: " + e.getMessage());
                }
            }
        }
    }

    public static void processClient(
            Socket clientSocket,
            BufferedReader clientInput,
            HashMap<String, String[]> map,
            ArrayList<OutputStream> replicaOutputStreams,
            String[] args,
            String serverRole) {
        try {
            BufferedWriter clientOutput = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            String content;
            ArrayList<String[]> bufferedCommands = new ArrayList<>();
            boolean encounteredMulti = false;
            HashMap<String, HashMap<String, HashMap<String, String>>> streams = new HashMap<>();
            HashMap<String, String[]> rdbMap = new HashMap<>();

            while ((content = clientInput.readLine()) != null) {
                if (content.charAt(0) == '*') {
                    int commandParams = Integer.parseInt(content.substring(1));
                    String[] params = new String[commandParams];
                    if (serverRole.equals("slave") && handshakeComplete) {
                        offset += content.length() + 2;
                    }
                    for (int i = 0; i < commandParams; i++) {
                        String commandLength = clientInput.readLine();
                        String curStr = clientInput.readLine();
                        if (serverRole.equals("slave") && handshakeComplete) {
                            offset += commandLength.length() + curStr.length() + 4;
                        }
                        params[i] = curStr;
                    }

                    if (params[0].equalsIgnoreCase("multi")) {
                        encounteredMulti = true;
                        clientOutput.write("+OK\r\n");
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("exec")) {
                        if (encounteredMulti) {
                            StringBuilder result = new StringBuilder("*" + bufferedCommands.size() + "\r\n");
                            while (!bufferedCommands.isEmpty()) {
                                String[] curCommand = bufferedCommands.remove(0);
                                String returnStr = executeCommand(clientSocket, clientOutput, map, curCommand, rdbMap, replicaOutputStreams, args, serverRole, false);
                                result.append(returnStr);
                            }
                            clientOutput.write(result.toString());
                            clientOutput.flush();
                            encounteredMulti = false;
                        } else {
                            clientOutput.write("-ERR EXEC without MULTI\r\n");
                            clientOutput.flush();
                        }
                    } else if (params[0].equalsIgnoreCase("discard")) {
                        if (encounteredMulti) {
                            bufferedCommands.clear();
                            encounteredMulti = false;
                            clientOutput.write("+OK\r\n");
                        } else {
                            clientOutput.write("-ERR DISCARD without MULTI\r\n");
                        }
                        clientOutput.flush();
                    } else if (encounteredMulti) {
                        bufferedCommands.add(params);
                        clientOutput.write("+QUEUED\r\n");
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("rpush")) {
                        ArrayList<String> appendList = listMap.computeIfAbsent(params[1], k -> new ArrayList<>());
                        for (int i = 2; i < params.length; i++) {
                            appendList.add(params[i]);
                        }
                        clientOutput.write(":" + appendList.size() + "\r\n");
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("lrange")) {
                        int startRange = Integer.parseInt(params[2]);
                        int endRange = Integer.parseInt(params[3]);
                        ArrayList<String> appendList = listMap.getOrDefault(params[1], new ArrayList<>());
                        if (appendList.isEmpty() || startRange >= appendList.size()) {
                            clientOutput.write("*0\r\n");
                        } else {
                            if (startRange < 0) startRange = Math.max(0, appendList.size() + startRange);
                            if (endRange < 0) endRange = Math.max(0, appendList.size() + endRange);
                            endRange = Math.min(endRange, appendList.size() - 1);
                            if (startRange > endRange) {
                                clientOutput.write("*0\r\n");
                            } else {
                                StringBuilder result = new StringBuilder("*" + (endRange - startRange + 1) + "\r\n");
                                for (int i = startRange; i <= endRange; i++) {
                                    String value = appendList.get(i);
                                    result.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                                }
                                clientOutput.write(result.toString());
                            }
                        }
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("lpush")) {
                        ArrayList<String> appendList = listMap.computeIfAbsent(params[1], k -> new ArrayList<>());
                        for (int i = 2; i < params.length; i++) {
                            appendList.add(0, params[i]);
                        }
                        clientOutput.write(":" + appendList.size() + "\r\n");
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("llen")) {
                        ArrayList<String> appendList = listMap.getOrDefault(params[1], new ArrayList<>());
                        clientOutput.write(":" + appendList.size() + "\r\n");
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("lpop")) {
                        ArrayList<String> appendList = listMap.getOrDefault(params[1], new ArrayList<>());
                        if (appendList.isEmpty()) {
                            clientOutput.write("$-1\r\n");
                        } else if (params.length == 2) { // LPOP key (no count)
                            String value = appendList.remove(0);
                            if (appendList.isEmpty()) listMap.remove(params[1]);
                            clientOutput.write("$" + value.length() + "\r\n" + value + "\r\n");
                        } else { // LPOP key count
                            int count = Integer.parseInt(params[2]);
                            count = Math.min(count, appendList.size());
                            StringBuilder result = new StringBuilder("*" + count + "\r\n");
                            for (int i = 0; i < count; i++) {
                                String value = appendList.remove(0);
                                result.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                            }
                            if (appendList.isEmpty()) listMap.remove(params[1]);
                            clientOutput.write(result.toString());
                        }
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("blpop")) {
                        synchronized (lock1) {
                            clientPriority.add(clientSocket);
                        }
                        try {
                            long timeout = (long) (Double.parseDouble(params[2]) * 1000);
                            long startTime = System.currentTimeMillis();
                            String key = params[1];
                            ArrayList<String> list;

                            while (true) {
                                synchronized (lock1) {
                                    list = listMap.getOrDefault(key, new ArrayList<>());
                                    if (!list.isEmpty() && clientSocket == clientPriority.get(0)) {
                                        String value = list.remove(0);
                                        if (list.isEmpty()) {
                                            listMap.remove(key);
                                        } else {
                                            listMap.put(key, list);
                                        }
                                        clientPriority.remove(0);
                                        String result = "*2\r\n"
                                                + "$" + key.length() + "\r\n" + key + "\r\n"
                                                + "$" + value.length() + "\r\n" + value + "\r\n";
                                        clientOutput.write(result);
                                        clientOutput.flush();
                                        break;
                                    }
                                }
                                if (timeout > 0 && System.currentTimeMillis() - startTime >= timeout) {
                                    synchronized (lock1) {
                                        clientPriority.remove(clientSocket);
                                    }
                                    clientOutput.write("$-1\r\n");
                                    clientOutput.flush();
                                    break;
                                }
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        } catch (IOException e) {
                            System.out.println("BLPOP error: " + e.getMessage());
                        }
                    } else if (params[0].equalsIgnoreCase("type")) {
                        if (map.containsKey(params[1])) {
                            clientOutput.write("+string\r\n");
                        } else if (streams.containsKey(params[1])) {
                            clientOutput.write("+stream\r\n");
                        } else {
                            clientOutput.write("+none\r\n");
                        }
                        clientOutput.flush();
                    } else if (params[0].equalsIgnoreCase("xadd")) {
                        HashMap<String, HashMap<String, String>> entries = streams.computeIfAbsent(params[1], k -> new HashMap<>());
                        HashMap<String, String> fields = entries.computeIfAbsent(params[2], k -> new HashMap<>());
                        fields.put(params[3], params[4]);
                        entries.put(params[2], fields);
                        streams.put(params[1], entries);
                        clientOutput.write("$" + params[2].length() + "\r\n" + params[2] + "\r\n");
                        clientOutput.flush();
                    } else {
                        executeCommand(clientSocket, clientOutput, map, params, rdbMap, replicaOutputStreams, args, serverRole, true);
                    }
                }
                handshakeComplete = true;
            }
        } catch (IOException e) {
            System.out.println("Client processing error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    public static String executeCommand(
            Socket clientSocket,
            BufferedWriter clientOutput,
            HashMap<String, String[]> map,
            String[] params,
            HashMap<String, String[]> rdbMap,
            ArrayList<OutputStream> replicaOutputStreams,
            String[] args,
            String serverRole,
            boolean write) {
        String fileName = args.length > 3 && args[0].equals("--dir") && args[2].equals("--dbfilename") ? args[1] + "/" + args[3] : "";
        try {
            if (params[0].equalsIgnoreCase("ping")) {
                String response = "+PONG\r\n";
                if (serverRole.equals("master") && write) {
                    clientOutput.write(response);
                    clientOutput.flush();
                }
                return response;
            } else if (params[0].equalsIgnoreCase("echo")) {
                String response = "$" + params[1].length() + "\r\n" + params[1] + "\r\n";
                if (write) {
                    clientOutput.write(response);
                    clientOutput.flush();
                }
                return response;
            } else if (params[0].equalsIgnoreCase("set")) {
                long curTime = System.currentTimeMillis();
                String key = params[1];
                String value = params[2];
                String[] record = params.length > 3 ? new String[]{value, String.valueOf(curTime), params[params.length - 1]} : new String[]{value, "", ""};
                map.put(key, record);
                if (serverRole.equals("master")) {
                    previousWrite = true;
                    for (OutputStream replicaStream : replicaOutputStreams) {
                        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(replicaStream));
                        br.write("*3\r\n$3\r\nSET\r\n$" + key.length() + "\r\n" + key + "\r\n$" + value.length() + "\r\n" + value + "\r\n");
                        br.flush();
                    }
                    if (write) {
                        clientOutput.write("+OK\r\n");
                        clientOutput.flush();
                    }
                }
                return "+OK\r\n";
            } else if (params[0].equalsIgnoreCase("get")) {
                long curTime = System.currentTimeMillis();
                String key = params[1];
                String response = "$-1\r\n";
                if (!fileName.isEmpty()) {
                    File file = new File(fileName);
                    if (file.exists()) {
                        try (FileInputStream fis = new FileInputStream(file)) {
                            int bit, count = 0;
                            String curWord = "";
                            int wordCount = 0;
                            String keyString = "";
                            boolean valid = false;
                            long expiryTime = -1;
                            while ((bit = fis.read()) != -1) {
                                if (count >= 45 && count <= file.length() - 9) {
                                    if ((bit < 65 || bit > 90) && (bit < 97 || bit > 122)) {
                                        if (valid) {
                                            wordCount++;
                                            if (wordCount % 2 == 1) {
                                                keyString = curWord;
                                            } else {
                                                String[] record = new String[]{curWord, expiryTime != -1 ? String.valueOf(expiryTime) : ""};
                                                rdbMap.put(keyString, record);
                                            }
                                            curWord = "";
                                        }
                                        valid = false;
                                        if (bit == 252) {
                                            int[] buffer = new int[8];
                                            for (int i = 0; i < 8; i++) {
                                                buffer[i] = fis.read();
                                                count++;
                                            }
                                            long value = 0;
                                            for (int i = buffer.length - 1; i >= 0; i--) {
                                                value = (value << 8) | (buffer[i] & 0xFF);
                                            }
                                            expiryTime = value;
                                        }
                                    } else {
                                        valid = true;
                                        curWord += (char) bit;
                                    }
                                }
                                count++;
                            }
                        }
                        String[] record = rdbMap.get(key);
                        if (record != null) {
                            if (record[1].isEmpty() || curTime <= Long.parseLong(record[1])) {
                                response = "$" + record[0].length() + "\r\n" + record[0] + "\r\n";
                            }
                        }
                    }
                } else if (map.containsKey(key)) {
                    String[] record = map.get(key);
                    if (record[1].isEmpty() || curTime <= Long.parseLong(record[1]) + Long.parseLong(record[2])) {
                        response = "$" + record[0].length() + "\r\n" + record[0] + "\r\n";
                    }
                }
                if (write) {
                    clientOutput.write(response);
                    clientOutput.flush();
                }
                return response;
            } else if (params[0].equalsIgnoreCase("config")) {
                String dirName = args.length > 1 ? args[1] : "";
                String dbFileName = args.length > 3 ? args[3] : "";
                String response = "";
                if (params[2].equals("dir")) {
                    response = "*2\r\n$3\r\ndir\r\n$" + dirName.length() + "\r\n" + dirName + "\r\n";
                } else if (params[2].equals("dbfilename")) {
                    response = "*2\r\n$10\r\ndbfilename\r\n$" + dbFileName.length() + "\r\n" + dbFileName + "\r\n";
                }
                if (write) {
                    clientOutput.write(response);
                    clientOutput.flush();
                }
                return response;
            } else if (params[0].equalsIgnoreCase("keys")) {
                StringBuilder result = new StringBuilder();
                if (!fileName.isEmpty()) {
                    File file = new File(fileName);
                    if (file.exists()) {
                        try (FileInputStream fis = new FileInputStream(file)) {
                            int bit, count = 0, wordCount = 0;
                            String key = "";
                            boolean valid = false;
                            while ((bit = fis.read()) != -1) {
                                if (count >= 47 && count <= file.length() - 9) {
                                    if ((bit < 65 || bit > 90) && (bit < 97 || bit > 122)) {
                                        if (valid) {
                                            wordCount++;
                                            if (wordCount % 2 == 1) {
                                                result.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                                            }
                                            key = "";
                                        }
                                        valid = false;
                                    } else {
                                        valid = true;
                                        key += (char) bit;
                                    }
                                }
                                count++;
                            }
                            clientOutput.write("*" + (wordCount / 2) + "\r\n" + result);
                            clientOutput.flush();
                        }
                    } else {
                        clientOutput.write("*0\r\n");
                        clientOutput.flush();
                    }
                }
            } else if (params[0].equalsIgnoreCase("info")) {
                if (params[1].equals("replication")) {
                    String response = "$" + (83 + serverRole.length()) + "\r\n"
                            + "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
                            + "master_repl_offset:0\r\n"
                            + "role:" + serverRole + "\r\n";
                    clientOutput.write(response);
                    clientOutput.flush();
                }
            } else if (params[0].equalsIgnoreCase("replconf")) {
                if (params[1].equalsIgnoreCase("listening-port") || params[1].equalsIgnoreCase("capa")) {
                    clientOutput.write("+OK\r\n");
                    clientOutput.flush();
                } else if (params[1].equalsIgnoreCase("getack") && serverRole.equals("slave")) {
                    clientOutput.write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + String.valueOf(offset).length() + "\r\n" + offset + "\r\n");
                    clientOutput.flush();
                }
            } else if (params[0].equalsIgnoreCase("psync")) {
                clientOutput.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");
                clientOutput.flush();
                byte[] contents = HexFormat.of().parseHex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
                clientSocket.getOutputStream().write(("$" + contents.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
                clientSocket.getOutputStream().write(contents);
                clientSocket.getOutputStream().flush();
                replicaOutputStreams.add(clientSocket.getOutputStream());
            } else if (params[0].equalsIgnoreCase("wait")) {
                if (Integer.parseInt(params[1]) == 0) {
                    clientOutput.write(":0\r\n");
                    clientOutput.flush();
                } else {
                    long startTime = System.currentTimeMillis();
                    long timeout = Long.parseLong(params[2]);
                    int acknowledged = 0;
                    if (previousWrite) {
                        for (OutputStream replicaStream : replicaOutputStreams) {
                            replicaStream.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".getBytes());
                            replicaStream.flush();
                        }
                    }
                    while (System.currentTimeMillis() - startTime < timeout) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    clientOutput.write(":" + acknowledged + "\r\n");
                    clientOutput.flush();
                }
            } else if (params[0].equalsIgnoreCase("incr")) {
                String key = params[1];
                String[] valueRecord = map.getOrDefault(key, new String[]{"0", "", ""});
                try {
                    int value = Integer.parseInt(valueRecord[0]);
                    valueRecord[0] = String.valueOf(value + 1);
                    map.put(key, valueRecord);
                    String response = ":" + (value + 1) + "\r\n";
                    if (write) {
                        clientOutput.write(response);
                        clientOutput.flush();
                    }
                    return response;
                } catch (NumberFormatException e) {
                    String response = "-ERR value is not an integer or out of range\r\n";
                    if (write) {
                        clientOutput.write(response);
                        clientOutput.flush();
                    }
                    return response;
                }
            }
        } catch (IOException e) {
            System.out.println("Command execution error: " + e.getMessage());
        }
        return "";
    }
}