import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
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
        System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());
        
        // Start a new thread to handle the client
        ClientHandler clientHandler = new ClientHandler(clientSocket);
        new Thread(clientHandler).start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException when closing server: " + e.getMessage());
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

    // Constructor for list value (RPUSH)
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
      return expiryTime > 0 && System.currentTimeMillis() > expiryTime;
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

        // Read RESP commands in a loop
        String line;
        while ((line = reader.readLine()) != null) {
          // Expect RESP array starting with '*'
          if (!line.startsWith("*")) {
            outputStream.write("-ERR invalid RESP array\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Invalid RESP array: " + line);
            continue;
          }

          // Parse number of elements in the array
          int numElements;
          try {
            numElements = Integer.parseInt(line.substring(1));
          } catch (NumberFormatException e) {
            outputStream.write("-ERR invalid array length\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Invalid array length: " + line);
            continue;
          }

          // Read command and arguments
          String[] elements = new String[numElements];
          for (int i = 0; i < numElements; i++) {
            String bulkStringHeader = reader.readLine();
            if (bulkStringHeader == null || !bulkStringHeader.startsWith("$")) {
              outputStream.write("-ERR invalid bulk string header\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Invalid bulk string header: " + bulkStringHeader);
              continue;
            }

            int bulkStringLength;
            try {
              bulkStringLength = Integer.parseInt(bulkStringHeader.substring(1));
            } catch (NumberFormatException e) {
              outputStream.write("-ERR invalid bulk string length\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Invalid bulk string length: " + bulkStringHeader);
              continue;
            }

            String value = reader.readLine();
            if (value == null || value.length() != bulkStringLength) {
              outputStream.write("-ERR invalid bulk string value\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Invalid bulk string value: " + value);
              continue;
            }

            elements[i] = value;
          }

          // Process the command
          if (elements.length == 0) {
            outputStream.write("-ERR no command provided\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": No command provided");
            continue;
          }

          String command = elements[0];
          if ("PING".equalsIgnoreCase(command)) {
            if (numElements != 1) {
              outputStream.write("-ERR wrong number of arguments for PING\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for PING: " + numElements);
              continue;
            }
            outputStream.write("+PONG\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received PING, sent PONG");
          } else if ("ECHO".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for ECHO\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for ECHO: " + numElements);
              continue;
            }
            String response = "$" + elements[1].length() + "\r\n" + elements[1] + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received ECHO " + elements[1] + ", sent " + elements[1]);
          } else if ("SET".equalsIgnoreCase(command)) {
            if (numElements != 3 && numElements != 5) {
              outputStream.write("-ERR wrong number of arguments for SET\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for SET: " + numElements);
              continue;
            }
            String key = elements[1];
            String value = elements[2];
            long expiryTime = 0;
            if (numElements == 5) {
              if (!"PX".equalsIgnoreCase(elements[3])) {
                outputStream.write("-ERR invalid argument for SET\r\n".getBytes());
                outputStream.flush();
                System.out.println("Client " + clientAddr + ": Invalid argument for SET: " + elements[3]);
                continue;
              }
              try {
                long expiryMs = Long.parseLong(elements[4]);
                if (expiryMs <= 0) {
                  outputStream.write("-ERR invalid expiry time\r\n".getBytes());
                  outputStream.flush();
                  System.out.println("Client " + clientAddr + ": Invalid expiry time: " + elements[4]);
                  continue;
                }
                expiryTime = System.currentTimeMillis() + expiryMs;
              } catch (NumberFormatException e) {
                outputStream.write("-ERR invalid expiry time\r\n".getBytes());
                outputStream.flush();
                System.out.println("Client " + clientAddr + ": Invalid expiry time: " + elements[4]);
                continue;
              }
            }
            store.put(key, new ValueEntry(value, expiryTime));
            outputStream.write("+OK\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received SET " + key + " " + value + (expiryTime > 0 ? " with PX " + elements[4] : ""));
          } else if ("GET".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for GET\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for GET: " + numElements);
              continue;
            }
            String key = elements[1];
            ValueEntry entry = store.get(key);
            if (entry == null || entry.isExpired() || entry.isList()) {
              outputStream.write("$-1\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received GET " + key + ", sent null (missing, expired, or not a string)");
            } else {
              String value = entry.getStringValue();
              String response = "$" + value.length() + "\r\n" + value + "\r\n";
              outputStream.write(response.getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received GET " + key + ", sent " + value);
            }
          } else if ("RPUSH".equalsIgnoreCase(command)) {
            if (numElements < 3) {
              outputStream.write("-ERR wrong number of arguments for RPUSH\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for RPUSH: " + numElements);
              continue;
            }
            String key = elements[1];
            List<String> values = Arrays.asList(elements).subList(2, elements.length);
            // Atomically update the list
            store.compute(key, (k, existingEntry) -> {
              List<String> list;
              if (existingEntry == null || existingEntry.isExpired()) {
                list = new ArrayList<>(values);
              } else if (existingEntry.isList()) {
                list = new ArrayList<>(existingEntry.getListValue());
                list.addAll(values);
              } else {
                return existingEntry; // Will trigger error below
              }
              return new ValueEntry(list, 0); // No expiry for lists
            });
            ValueEntry entry = store.get(key);
            if (!entry.isList()) {
              outputStream.write("-ERR key exists and is not a list\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received RPUSH " + key + ", failed: key exists and is not a list");
              continue;
            }
            int length = entry.getListValue().size();
            String response = ":" + length + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received RPUSH " + key + " with " + values.size() + " elements, sent " + length);
          } else if ("LRANGE".equalsIgnoreCase(command)) {
            if (numElements != 4) {
              outputStream.write("-ERR wrong number of arguments for LRANGE\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Wrong number of arguments for LRANGE: " + numElements);
              continue;
            }
            String key = elements[1];
            long start, end;
            try {
              start = Long.parseLong(elements[2]);
              end = Long.parseLong(elements[3]);
              if (start < 0 || end < 0) {
                outputStream.write("-ERR index out of range\r\n".getBytes());
                outputStream.flush();
                System.out.println("Client " + clientAddr + ": Received LRANGE " + key + " " + elements[2] + " " + elements[3] + ", failed: negative index");
                continue;
              }
            } catch (NumberFormatException e) {
              outputStream.write("-ERR invalid index\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received LRANGE " + key + " " + elements[2] + " " + elements[3] + ", failed: invalid index");
              continue;
            }
            ValueEntry entry = store.get(key);
            if (entry == null || entry.isExpired() || !entry.isList()) {
              outputStream.write("*0\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received LRANGE " + key + " " + start + " " + end + ", sent empty array (missing, expired, or not a list)");
              continue;
            }
            List<String> list = entry.getListValue();
            if (start > end || start >= list.size()) {
              outputStream.write("*0\r\n".getBytes());
              outputStream.flush();
              System.out.println("Client " + clientAddr + ": Received LRANGE " + key + " " + start + " " + end + ", sent empty array (invalid range)");
              continue;
            }
            int adjustedEnd = (int) Math.min(end, list.size() - 1);
            List<String> range = list.subList((int) start, adjustedEnd + 1);
            StringBuilder response = new StringBuilder("*" + range.size() + "\r\n");
            for (String value : range) {
              response.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
            }
            outputStream.write(response.toString().getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received LRANGE " + key + " " + start + " " + end + ", sent " + range.size() + " elements");
          } else {
            outputStream.write("-ERR unknown command\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received unknown command: " + command);
          }
        }
        System.out.println("Client " + clientAddr + " disconnected");
      } catch (IOException e) {
        System.out.println("IOException for client " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
      } finally {
        try {
          if (reader != null) {
            reader.close();
          }
          if (outputStream != null) {
            outputStream.close();
          }
          if (clientSocket != null) {
            clientSocket.close();
          }
        } catch (IOException e) {
          System.out.println("IOException when closing client " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        }
      }
    }
  }
}