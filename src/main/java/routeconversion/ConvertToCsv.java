package routeconversion;
/*
Read objects from "route" and create/update routes.csv file for each row 1 waypoint, in order to have data for spark
 */
import config.StaticVars;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class ConvertToCsv {

    public static void main(String[] args) {
        try{
            FileInputStream f = new FileInputStream(StaticVars.routesOutputSource);
            ObjectInputStream o = new ObjectInputStream(f);

            ArrayList<Route> allRoutes = (ArrayList<Route>) o.readObject();
            o.close();
            FileWriter out = new FileWriter(StaticVars.routesOutputSource.concat(".csv"));
            try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
//                    .withHeader( String.join(",", Arrays.stream(RouteHeaders.values()).map(Object::toString).collect(Collectors.toList()))))) {
                    .withHeader(RouteHeaders.ROUTE_NAME.toString(),RouteHeaders.WAYPOINT_ID.toString(),RouteHeaders.LATITUDE.toString(),RouteHeaders.LONGTITUDE.toString()))) {
                for (Route route : allRoutes) {
                    if (route.getRoute().size()>0){
                        int index = 1;
                        for (Waypoint waypoint : route.getRoute()){
                            printer.printRecord(route.getName(),(index++),waypoint.getLatitude(),waypoint.getLongitude());
                        }
                    }
                }
            }
            allRoutes.forEach(route -> System.out.println(route.getName()));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
