package com.config;

import java.io.*;
import java.net.Socket;



public class ServerTest {
    public static void main(String[] args) throws IOException {

                String hostName = args[0];
                int portNumber = Integer.parseInt(args[1]);

                Socket socket = new Socket(hostName,portNumber);
                BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream() ));

            while (true){
                in.lines().forEach(line -> System.out.println(line));
            }

    }
}
