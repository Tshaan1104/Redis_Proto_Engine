import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
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
          String command = null;
          String argument = null;
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

            if (i == 0) {
              command = value;
            } else if (i == 1) {
              argument = value;
            }
          }

          // Process the command
          if (command == null) {
            outputStream.write("-ERR no command provided\r\n".getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": No command provided");
            continue;
          }

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
            String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
            outputStream.write(response.getBytes());
            outputStream.flush();
            System.out.println("Client " + clientAddr + ": Received ECHO " + argument + ", sent " + argument);
          } else {
outputStream.write(("-ERR unknown command '" + command + "'\r\n").getBytes());
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
          }//sdf
        } catch (IOException e) {
          System.out.println("IOException when closing client " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        }
      }
    }
  }
}