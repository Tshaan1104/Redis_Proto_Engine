import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
  // Thread-safe key-value store with expiry
  private static final ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
  // Thread-safe map of list keys to queues of blocked clients
  private static final ConcurrentHashMap<String, LinkedBlockingQueue<ClientHandler>> blockedClients = new ConcurrentHashMap<>();

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
    private volatile boolean isRunning = true;
    // Queue for this client to receive notifications (popped key and value)
    private final LinkedBlockingQueue<String[]> notificationQueue = new LinkedBlockingQueue<>();

    public ClientHandler(Socket socket) {
      this.clientSocket = socket;
    }

    // Method to notify this client with a popped key and value
    public void notifyClient(String key, String value) {
      try {
        System.out.println(clientSocket.getRemoteSocketAddress() + ": Attempting to notify with key " + key + ", value " + value);
        notificationQueue.put(new String[]{key, value});
        System.out.println(clientSocket.getRemoteSocketAddress() + ": Notified with key " + key + ", value " + value);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println(clientSocket.getRemoteSocketAddress() + ": Interrupted while notifying: " + e.getMessage());
      }
    }

    // Method to remove this client from all blocking queues
    private void removeFromBlockingQueues() {
      for (LinkedBlockingQueue<ClientHandler> queue : blockedClients.values()) {
        queue.remove(this);
      }
    }

    @Override
    public void run() {
      BufferedReader reader = null;
      OutputStream outputStream = null;
      String clientAddr = clientSocket.getInetAddress() != null ? clientSocket.getInetAddress().getHostAddress() : "unknown";
      try {
        // Get input and output streams
        reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        outputStream = clientSocket.getOutputStream();

        // Read RESP commands
        String line;
        while (isRunning && (line = reader.readLine()) != null) {
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
            outputStream.write(("-ERR invalid array length: " + line + "\r\n").getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": Invalid array length: " + e.getMessage());
            continue;
          }

          // Read command elements
          String[] elements = new String[numElements];
          for (int i = 0; i < numElements; i++) {
            String bulkStringHeader = reader.readLine();
            if (bulkStringHeader == null) {
              throw new IOException("Client disconnected during bulk string header read");
            }
            if (!bulkStringHeader.startsWith("$")) {
              outputStream.write("-ERR invalid bulk string header\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid bulk header: " + bulkStringHeader);
              continue;
            }

            int bulkStringLength;
            try {
              bulkStringLength = Integer.parseInt(bulkStringHeader.substring(1));
            } catch (NumberFormatException e) {
              outputStream.write(("-ERR invalid bulk string length: " + bulkStringHeader + "\r\n").getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid bulk length: " + e.getMessage());
              continue;
            }

            String value = reader.readLine();
            if (value == null) {
              throw new IOException("Client disconnected during bulk string value read");
            }
            if (value.length() != bulkStringLength) {
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
          } else if ("TYPE".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for TYPE\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong TYPE args: " + numElements);
              continue;
            }
            String key = elements[1];
            ValueEntry entry = store.get(key);
            String type;
            if (entry == null || entry.isExpired()) {
              type = "none";
            } else if (entry.isList()) {
              type = "list";
            } else {
              type = "string";
            }
            String response = "+" + type + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": TYPE " + key + " -> " + type);
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
            // Notify blocked clients
            notifyBlockedClients(key);
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
            // Notify blocked clients
            notifyBlockedClients(key);
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
          } else if ("LLEN".equalsIgnoreCase(command)) {
            if (numElements != 2) {
              outputStream.write("-ERR wrong number of arguments for LLEN\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong LLEN args: " + numElements);
              continue;
            }
            String key = elements[1];
            ValueEntry entry = store.get(key);
            int length = 0;
            if (entry != null && !entry.isExpired() && entry.isList()) {
              length = entry.getListValue().size();
            }
            String response = ":" + length + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": LLEN " + key + " -> " + length);
          } else if ("LPOP".equalsIgnoreCase(command)) {
            if (numElements != 2 && numElements != 3) {
              outputStream.write("-ERR wrong number of arguments for LPOP\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong LPOP args: " + numElements);
              continue;
            }
            String key = elements[1];
            final int finalCount;
            if (numElements == 3) {
              try {
                finalCount = Integer.parseInt(elements[2]);
                if (finalCount < 0) {
                  outputStream.write("-ERR count must be non-negative\r\n".getBytes());
                  outputStream.flush();
                  System.out.println(clientAddr + ": LPOP " + key + " failed: negative count " + elements[2]);
                  continue;
                }
              } catch (NumberFormatException e) {
                outputStream.write("-ERR invalid count\r\n".getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": LPOP " + key + " failed: invalid count " + elements[2]);
                continue;
              }
            } else {
              finalCount = 1; // Default count for LPOP without count argument
            }
            List<String> poppedValues = new ArrayList<>(); // To capture popped values
            store.compute(key, (k, existing) -> {
              if (existing == null || existing.isExpired() || !existing.isList() || existing.getListValue().isEmpty()) {
                return existing;
              }
              List<String> list = new ArrayList<>(existing.getListValue());
              int elementsToRemove = Math.min(finalCount, list.size());
              for (int i = 0; i < elementsToRemove; i++) {
                poppedValues.add(list.remove(0));
              }
              return new ValueEntry(list, list.isEmpty() ? existing.expiryTime : 0);
            });
            if (numElements == 2) {
              // Single-element LPOP returns a bulk string
              if (poppedValues.isEmpty()) {
                outputStream.write("$-1\r\n".getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": LPOP " + key + " -> null");
              } else {
                String value = poppedValues.get(0);
                String response = "$" + value.length() + "\r\n" + value + "\r\n";
                outputStream.write(response.getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": LPOP " + key + " -> " + value);
              }
            } else {
              // Multi-element LPOP returns an array
              StringBuilder response = new StringBuilder("*" + poppedValues.size() + "\r\n");
              for (String value : poppedValues) {
                response.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
              }
              outputStream.write(response.toString().getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": LPOP " + key + " " + finalCount + " -> " + poppedValues.size() + " elements");
            }
          } else if ("BLPOP".equalsIgnoreCase(command)) {
            if (numElements != 3) {
              outputStream.write("-ERR wrong number of arguments for BLPOP\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Wrong BLPOP args: " + numElements);
              continue;
            }
            String key = elements[1];
            String timeoutStr = elements[2];
            float timeout;
            try {
              timeout = Float.parseFloat(timeoutStr);
              if (timeout < 0) {
                outputStream.write("-ERR timeout must be non-negative\r\n".getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": Wrong BLPOP timeout: " + timeoutStr);
                continue;
              }
            } catch (NumberFormatException e) {
              outputStream.write("-ERR invalid timeout\r\n".getBytes());
              outputStream.flush();
              System.out.println(clientAddr + ": Invalid BLPOP timeout: " + timeoutStr);
              continue;
            }
            // Check socket state before proceeding
            if (clientSocket.isClosed()) {
              System.out.println(clientAddr + ": BLPOP " + key + " socket closed before processing");
              break;
            }
            // Check if list has elements immediately
            ValueEntry entry = store.get(key);
            if (entry != null && !entry.isExpired() && entry.isList() && !entry.getListValue().isEmpty()) {
              List<String> poppedValues = new ArrayList<>();
              store.compute(key, (k, existing) -> {
                if (existing == null || existing.isExpired() || !existing.isList() || existing.getListValue().isEmpty()) {
                  return existing;
                }
                List<String> list = new ArrayList<>(existing.getListValue());
                poppedValues.add(list.remove(0));
                return new ValueEntry(list, list.isEmpty() ? existing.expiryTime : 0);
              });
              String value = poppedValues.get(0);
              String response = "*2\r\n$" + key.length() + "\r\n" + key + "\r\n$" + value.length() + "\r\n" + value + "\r\n";
              if (!clientSocket.isClosed()) {
                outputStream.write(response.getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": BLPOP " + key + " -> [" + key + ", " + value + "]");
              } else {
                System.out.println(clientAddr + ": BLPOP " + key + " socket closed, cannot send response");
                break;
              }
              continue;
            }
            // Block until an element is available or timeout expires
            LinkedBlockingQueue<ClientHandler> queue = blockedClients.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
            try {
              queue.put(this); // Add client to blocking queue
              System.out.println(clientAddr + ": BLPOP " + key + " blocking for " + timeout + " seconds, queue size: " + queue.size());
              long timeoutMs = (long) (timeout * 1000);
              // For timeout == 0.0, use non-blocking poll
              String[] result = timeout == 0 ? notificationQueue.poll(0, TimeUnit.MILLISECONDS) : notificationQueue.poll(timeoutMs > 0 ? timeoutMs : 1, TimeUnit.MILLISECONDS);
              System.out.println(clientAddr + ": BLPOP " + key + " poll result: " + (result == null ? "null" : Arrays.toString(result)));
              // Re-check list if poll returns null for timeout == 0.0
              if (result == null && timeout == 0) {
                entry = store.get(key);
                if (entry != null && !entry.isExpired() && entry.isList() && !entry.getListValue().isEmpty()) {
                  List<String> poppedValues = new ArrayList<>();
                  store.compute(key, (k, existing) -> {
                    if (existing == null || existing.isExpired() || !existing.isList() || existing.getListValue().isEmpty()) {
                      return existing;
                    }
                    List<String> list = new ArrayList<>(existing.getListValue());
                    poppedValues.add(list.remove(0));
                    return new ValueEntry(list, list.isEmpty() ? existing.expiryTime : 0);
                  });
                  queue.remove(this);
                  if (!poppedValues.isEmpty()) {
                    String value = poppedValues.get(0);
                    String response = "*2\r\n$" + key.length() + "\r\n" + key + "\r\n$" + value.length() + "\r\n" + value + "\r\n";
                    if (!clientSocket.isClosed()) {
                      outputStream.write(response.getBytes());
                      outputStream.flush();
                      System.out.println(clientAddr + ": BLPOP " + key + " re-checked -> [" + key + ", " + value + "]");
                    } else {
                      System.out.println(clientAddr + ": BLPOP " + key + " re-check socket closed, cannot send response");
                      break;
                    }
                    continue;
                  }
                }
              }
              // Remove client from queue only if it was not served
              if (result == null) {
                queue.remove(this);
                if (!clientSocket.isClosed()) {
                  outputStream.write("$-1\r\n".getBytes());
                  outputStream.flush();
                  System.out.println(clientAddr + ": BLPOP " + key + " timed out after " + timeout + " seconds, queue size: " + queue.size());
                } else {
                  System.out.println(clientAddr + ": BLPOP " + key + " timeout socket closed, cannot send response");
                  break;
                }
                continue;
              }
              queue.remove(this); // Remove client after successful notification
              String poppedKey = result[0];
              String value = result[1];
              String response = "*2\r\n$" + poppedKey.length() + "\r\n" + poppedKey + "\r\n$" + value.length() + "\r\n" + value + "\r\n";
              if (!clientSocket.isClosed()) {
                outputStream.write(response.getBytes());
                outputStream.flush();
                System.out.println(clientAddr + ": BLPOP " + key + " unblocked -> [" + poppedKey + ", " + value + "], queue size: " + queue.size());
              } else {
                System.out.println(clientAddr + ": BLPOP " + key + " unblocked socket closed, cannot send response");
                break;
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              queue.remove(this);
              System.out.println(clientAddr + ": BLPOP " + key + " interrupted: " + e.getMessage());
            } catch (IOException e) {
              queue.remove(this);
              System.out.println(clientAddr + ": BLPOP " + key + " IOException: " + e.getMessage());
              break;
            }
          } else {
            outputStream.write(("-ERR unknown command: " + command + "\r\n").getBytes());
            outputStream.flush();
            System.out.println(clientAddr + ": Unknown command: " + command);
          }
        }
      } catch (IOException e) {
        System.out.println(clientAddr + ": IOException in run: " + e.getMessage());
      } finally {
        isRunning = false;
        removeFromBlockingQueues();
        try {
          if (reader != null) reader.close();
          if (outputStream != null) outputStream.close();
          if (clientSocket != null && !clientSocket.isClosed()) clientSocket.close();
        } catch (IOException e) {
          System.out.println(clientAddr + ": IOException closing: " + e.getMessage());
        }
        System.out.println(clientAddr + ": Disconnected");
      }
    }

    // Notify blocked clients for a given key
    private void notifyBlockedClients(String key) {
      LinkedBlockingQueue<ClientHandler> queue = blockedClients.get(key);
      if (queue == null || queue.isEmpty()) {
        System.out.println("notifyBlockedClients: No clients blocked for key " + key);
        return;
      }
      ValueEntry entry = store.get(key);
      if (entry == null || entry.isExpired() || !entry.isList() || entry.getListValue().isEmpty()) {
        System.out.println("notifyBlockedClients: No valid list for key " + key);
        return;
      }
      synchronized (queue) { // Synchronize to prevent race conditions
        ClientHandler client = queue.peek();
        if (client == null || !client.isRunning || client.clientSocket.isClosed()) {
          queue.poll();
          System.out.println("notifyBlockedClients: Removed disconnected client for key " + key + ", queue size: " + queue.size());
          notifyBlockedClients(key); // Recurse for next client
          return;
        }
        List<String> poppedValues = new ArrayList<>();
        store.compute(key, (k, existing) -> {
          if (existing == null || existing.isExpired() || !existing.isList() || existing.getListValue().isEmpty()) {
            return existing;
          }
          List<String> list = new ArrayList<>(existing.getListValue());
          poppedValues.add(list.remove(0));
          return new ValueEntry(list, list.isEmpty() ? existing.expiryTime : 0);
        });
        if (!poppedValues.isEmpty()) {
          queue.poll(); // Remove served client
          client.notifyClient(key, poppedValues.get(0));
          System.out.println(client.clientSocket.getRemoteSocketAddress() + ": Notified for key " + key + " with value " + poppedValues.get(0) + ", queue size: " + queue.size());
        }
      }
      // Clean up empty queue
      blockedClients.compute(key, (k, q) -> q == null || q.isEmpty() ? null : q);
    }
  }
}