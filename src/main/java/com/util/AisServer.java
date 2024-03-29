package com.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Stream;

public class AisServer {

    //full stream non-stop
    public static void main(String[] args) throws IOException, InterruptedException {
        int portNumber = Integer.parseInt(args[0]);

        ServerSocket echoSocket = new ServerSocket(portNumber);
        Socket socket = echoSocket.accept();

        while (true){
            File[] filesInDirectory = new File(args[1]).listFiles();
            Arrays.sort(filesInDirectory);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            for(File f : filesInDirectory){
                String filePath = f.getAbsolutePath();
                String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1,filePath.length());
                if("csv".equals(fileExtenstion)){
                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        // Skip the first line (the header)
                        br.readLine();
                        Stream<String> lines = br.lines();
                        lines.forEach(line -> {
                            String value = line;
//                            System.out.println(value);
                            out.println(line);

                        });
                    } catch (IOException e) {
                        System.out.println("Error reading file: " + filePath);
                    }
                }
            }

        }
    }
    //stream from excel depending on the datetime
    public static void mainOld(String[] args) throws IOException, InterruptedException {

        int portNumber = Integer.parseInt(args[0]);

        ServerSocket echoSocket = new ServerSocket(portNumber);
        Socket socket = echoSocket.accept();

        String[] HEADERS = { "mmsi","imo_nr","length","date_time_utc","lon","lat","sog","cog","true_heading","nav_status","message_nr"};

        String batchDate="";

        while (true){

          File[] filesInDirectory = new File(args[1]).listFiles();
          Arrays.sort(filesInDirectory);
          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

          for(File f : filesInDirectory){
              String filePath = f.getAbsolutePath();
              String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1,filePath.length());
              if("csv".equals(fileExtenstion)){
//                  System.out.println("CSV file found -> " + filePath);
                  // Call the method checkForCobalt(filePath);
                  Reader in = new FileReader(filePath);

                  Iterable<CSVRecord> records = CSVFormat.DEFAULT
                          .withDelimiter(';')
                          .withHeader(HEADERS)
                          .withFirstRecordAsHeader()
                          .parse(in);

                  for (CSVRecord record : records) {
                      String recordDate =  record.get("date_time_utc");

                      if (batchDate.length()==0){
                          batchDate = recordDate;
                      }

                      if (getDiffrenceInMins(batchDate,recordDate)>300){
                          batchDate = recordDate;
                          Thread.sleep(5000);
                      }

                      out.println(String.format(record.get("mmsi")+","+
                                      record.get("imo_nr")+","+
                                      record.get("length")+","+
                                      record.get("date_time_utc")+","+
                                      record.get("lon")+","+
                                      record.get("lat")+","+
                                      record.get("sog")+","+
                                      record.get("cog")+","+
                                      record.get("true_heading")+","+
                                      record.get("nav_status")+","+
                                      record.get("message_nr")
                              ));
                  }
              }
          }
          Thread.sleep(5000);
      }
    }

    static Long getDiffrenceInMins(String start, String end){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date d1 = sdf.parse(start);
            Date d2 = sdf.parse(end);
            return  (d2.getTime() - d1.getTime()) / (1000 * 60);
        }catch (ParseException e) {
            e.printStackTrace();
            return 0L;
        }
    }

}
