import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  // Thread-safe key-value store with expiry
  private static final ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      System.out.println("Server started on port " + port);

      // Continuously accept new client connections
      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
        
        // Start a new thread to handle the client
        ClientHandler clientSocketHandler = new ClientHandler(clientSocket);
        new Thread(clientSocketHandler).start();
      }
    } catch (IOException e) {
      e.printStackTrace(System.err);
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException e) {
        e.printStackTrace(System.err);
        System.out.println("IOException closing server: " + e.getMessage());
      }
    }
}

  // Class to store either a string value or a list with expiry time
  static class ValueEntry {
    private final String stringValue;
    private final List<String> listValue;
    private final long expiryTime; // Expiry timestamp in milliseconds, 0 if no expiry
    private final boolean isList;

    // Constructor for string value (SET/GET)
    public ValueEntry(String value, long expiryTime) {
      this.stringValue = value;
      this.listValue = null;
      this.expiryTime = expiryTime;
      this.isList = false;
    }

    // Constructor for list value (RPUSH/LPUSH)
    public ValueEntry(List<String> listValue, long expiryTime) {
      this.stringValue = null;
      this.listValue = listValue;
      this.expiryTime = expiryTime;
      this.isList = true;
    }

    public String getStringValue() {
      return stringValue;
    }

    public List<String> getListValue() {
      return listValue;
    }

    public boolean isList() {
      return isList;
    }

    public boolean isExpired() {
      return expiryTime > 0 && System.currentTimeMillis() >= expiryTime;
    }
  }

  static class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket socket) {
      this.clientSocket = socket;
    }

    @Override
    public void run() {
      BufferedReader reader = null;
      OutputStream outputStream = null;
      try {
        // Get input and output streams
        reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        outputStream = clientSocket.getOutputStream();
        String clientAddr = clientSocket.getRemoteSocketAddress().toString();

        // Read RESP commands
        String line;
        while ((line = reader.readLine()) != null) {
          // Expect RESP array starting with '*'
          if (!line.startsWith("*")) {
            outputStream.write("-ERR invalid RESP array\r\n".getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": Invalid array: " + line);
            continue;
          }

          // Parse number of elements
          int numElements;
          try {
            numElements = Integer.parseInt(line.substring(1));
          } catch (NumberFormatException e) {
            String ll="-ERR invalid array length: " + line + "\r\n";
            outputStream.write(ll.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": Invalid array length: " + e.getMessage());
            continue;
          }

          // Read command elements
          String[] elements = new String[numElements];
          for (int i = 0; i < numElements; i++) {
            String bulkStringHeader = reader.readLine();
            if (bulkStringHeader == null || !bulkStringHeader.startsWith("$")) {
              outputStream.write("-ERR invalid bulk string header\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid bulk header: " + bulkStringHeader);
              continue;
            }

            int bulkStringLength;
            try {
              bulkStringLength = Integer.parseInt(bulkStringHeader.substring(1));
            } catch (NumberFormatException e) {
              String ll="-ERR invalid bulk string length: " + bulkStringHeader + "\r\n";
              outputStream.write(ll.getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid bulk length: " + e.getMessage());
              continue;
            }

            String value = reader.readLine();
            if (value == null || value.length() != bulkStringLength) {
              outputStream.write("-ERR invalid bulk string value\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid bulk value: " + value);
              continue;
            }

            elements[i] = value;
          }

          // Process command
          if (elements.length == 0) {
            outputStream.write("-ERR no command provided\r\n".getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": No command provided");
            continue;
          }

          String command = elements[0];
          if ("PING".equalsIgnoreCase(command)) {
            if (numElements != 1) {
              outputStream.write("-ERR wrong number of arguments for PING\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong PING args: " + numElements);
              continue;
            }
            outputStream.write("+PONG\r\n".getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": PING -> PONG");
          } else if ("ECHO".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for ECHO\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong ECHO args: " + numElements);
              continue;
            }
            String response = "$" + elements[1].length() + "\r\n" + elements[1] + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": ECHO " + elements[1]);
          } else if ("SET".equalsIgnoreCase(command)) {
            if (numElements != 3 && numElements != 5) {
              outputStream.write("-ERR wrong number of arguments for SET\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong SET args: " + numElements);
              continue;
            }
            String key = elements[1];
            String value = elements[2];
            long expiryTime = 0;
            if (numElements == 5) {
              if (!"PX".equalsIgnoreCase(elements[3])) {
                outputStream.write("-ERR invalid argument for SET\r\n".getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": Invalid SET arg: " + elements[3]);
                continue;
              }
              try {
                long expiryMs = Long.parseLong(elements[4]);
                if (expiryMs <= 0) {
                  outputStream.write("-ERR invalid expiry time\r\n".getBytes());
                  outputStream.flush();
                  System.out.println(clientAddr + ": Invalid expiry: " + elements[4]);
                  continue;
                }
                expiryTime = System.currentTimeMillis() + expiryMs;
              } catch (NumberFormatException e) {
                outputStream.write("-ERR invalid expiry time\r\n".getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": Invalid expiry: " + elements[4]);
                continue;
              }
            }
            store.put(key, new ValueEntry(value, expiryTime));
            outputStream.write("+OK\r\n".getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": SET " + key + " " + value + (expiryTime > 0 ? " PX " + elements[4] : ""));
          } else if ("GET".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for GET\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong GET args: " + numElements);
              continue;
            }
            String key = elements[1];
            ValueEntry entry = store.get(key);
            if (entry == null || entry.isExpired() || entry.isList()) {
              outputStream.write("$-1\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": GET " + key + " -> null");
            } else {
              String value = entry.getStringValue();
              String response = "$" + value.length() + "\r\n" + value + "\r\n";
              outputStream.write(response.getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": GET " + key + " -> " + value);
            }
          } else if ("RPUSH".equalsIgnoreCase(command)) {
            if (numElements < 3) {
              outputStream.write("-ERR wrong number of arguments for RPUSH\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong RPUSH args: " + numElements);
              continue;
            }
            String key = elements[1];
            List<String> values = Arrays.asList(elements).subList(2, elements.length);
            store.compute(key, (k, existingEntry) -> {
              List<String> list;
              if (existingEntry == null || existingEntry.isExpired()) {
                list = new ArrayList<>(values);
              } else if (existingEntry.isList()) {
                list = new ArrayList<>(existingEntry.getListValue());
                list.addAll(values);
              } else {
                return existingEntry;
              }
              return new ValueEntry(list, 0);
            });
            ValueEntry entry = store.get(key);
            if (!entry.isList()) {
              outputStream.write("-ERR key exists and is not a list\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": RPUSH " + key + " failed: not a list");
              continue;
            }
            int length = entry.getListValue().size();
            String response = ":" + length + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": RPUSH " + key + " " + values.size() + " elements -> " + length);
          } else if ("LPUSH".equalsIgnoreCase(command)) {
            if (numElements < 3) {
              outputStream.write("-ERR wrong number of arguments for LPUSH\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong LPUSH args: " + numElements);
              continue;
            }
            String key = elements[1];
            List<String> values = new ArrayList<>(Arrays.asList(elements).subList(2, elements.length));
            Collections.reverse(values); // Reverse to match tester's expected order
            store.compute(key, (k, existing) -> {
              List<String> list;
              if (existing == null || existing.isExpired()) {
                list = new ArrayList<>(values);
              } else if (existing.isList()) {
                list = new ArrayList<>(existing.getListValue());
                list.addAll(0, values);
              } else {
                return existing;
              }
              return new ValueEntry(list, 0);
            });
            ValueEntry entry = store.get(key);
            if (!entry.isList()) {
              outputStream.write("-ERR key exists and is not a list\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": LPUSH " + key + " failed: not a list");
              continue;
            }
            int length = entry.getListValue().size();
            String response = ":" + length + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": LPUSH " + key + " " + values.size() + " elements (reversed) -> " + length);
          } else if ("LRANGE".equalsIgnoreCase(command)) {
            if (numElements != 4) {
              outputStream.write("-ERR wrong number of arguments for LRANGE\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong LRANGE args: " + numElements);
              continue;
            }
            String key = elements[1];
            long start, end;
            try {
              start = Long.parseLong(elements[2]);
              end = Long.parseLong(elements[3]);
            } catch (NumberFormatException e) {
              outputStream.write("-ERR invalid index\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": LRANGE " + key + " " + elements[2] + " " + elements[3] + " failed: invalid index");
              continue;
            }
            ValueEntry entry = store.get(key);
            if (entry == null || entry.isExpired() || !entry.isList()) {
              outputStream.write("*0\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": LRANGE " + key + " " + start + " " + end + " -> empty (missing/expired/not list)");
              continue;
            }
            List<String> list = entry.getListValue();
            int listSize = list.size();
            if (start < 0) {
              start = Math.max(0, listSize + start);
            }
            if (end < 0) {
              end = Math.max(0, listSize + end);
            }
            if (start > end || start >= listSize) {
              outputStream.write("*0\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": LRANGE " + key + " " + start + " " + end + " -> empty (invalid range)");
              continue;
            }
            int adjustedEnd = (int) Math.min(end, listSize - 1);
            List<String> range = list.subList((int) start, adjustedEnd + 1);
            StringBuilder response = new StringBuilder("*" + range.size() + "\r\n");
            for (String value : range) {
              response.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
            }
            outputStream.write(response.toString().getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": LRANGE " + key + " " + start + " " + end + " -> " + range.size() + " elements");
          } else {
            String ll="-ERR unknown command: " + command + "\r\n";
            outputStream.write(ll.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": Unknown command: " + command);
          }
        }
        System.out.println(clientAddr + ": Disconnected");
      } catch (IOException e) {
        String clientAddr = clientSocket.getInetAddress().getHostAddress();

        System.out.println(clientAddr + ": IOException: " + e.getMessage());
      } finally {
        try {
          if (reader != null) reader.close();
          if (outputStream != null) outputStream.close();
          if (clientSocket != null) clientSocket.close();
        } catch (IOException e) {
          String clientAddr = clientSocket.getInetAddress().getHostAddress();

          System.out.println(clientAddr + ": IOException closing: " + e.getMessage());
        }
      }
    }
  }
}