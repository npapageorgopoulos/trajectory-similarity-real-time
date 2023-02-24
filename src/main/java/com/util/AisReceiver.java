package com.util;
import com.config.StaticVars;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AisReceiver extends Receiver<String> {

    private  String host;
    private  int port;


    public AisReceiver(String host, int port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }
    public AisReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {}

    private void receive() {
        try {
//            Socket socket = new Socket(host, port);
            File[] filesInDirectory = new File(StaticVars.dataAIS).listFiles();
            Arrays.sort(filesInDirectory);
            BufferedReader reader = null;
            while (!isStopped()) {
                for (File f : filesInDirectory) {
                    String filePath = f.getAbsolutePath();
                    String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
//            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    if ("csv".equals(fileExtenstion)) {
                        reader = new BufferedReader(new FileReader(filePath));
                        String inputLine;
                        List<String> buffer = new ArrayList<>();
                        while ((inputLine = reader.readLine()) != null && !isStopped()) {
                            buffer.add(inputLine);
                            if (buffer.size() >= StaticVars.recordsBuffer) {
                                store(buffer.iterator());
                                buffer.clear();
                            Thread.sleep(1000);
                            }
                        }
                        if (!buffer.isEmpty()) {
                            store(buffer.iterator());
                            buffer.clear();
                        }
                    }
                }
            }
            reader.close();
//                    socket.close();
            restart("Trying to connect again");
        } catch (Exception e) {
            restart("Error receiving data", e);
        }
    }

}
