package routeconversion;
/*
Read  rtz files from "Routepath" convert to json and output as Route object to "route" file
 */

import config.StaticVars;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConvertRouteObjects {
    public static void main(String[] args) {
        ArrayList<Route>  allRoutes = new ArrayList<>();
        try {

            List<String> files = getRtzFilePaths(StaticVars.routesInputSource,1);
            for (String f : files){
//                JSONObject xmlJSONObj = XML.toJSONObject(Files.readString(Path.of(f))); //Java11
                JSONObject xmlJSONObj = XML.toJSONObject(Files.newBufferedReader(Paths.get(f)));
                JSONObject routeInfo = xmlJSONObj.getJSONObject("route").getJSONObject("routeInfo");
                Route route = new Route(routeInfo.getString("routeName"),
                        routeInfo.getString("validityPeriodStart"),
                        routeInfo.getString("validityPeriodStop"),
                        routeInfo.getString("vesselName"),
                        routeInfo.getString("vesselVoyage"),
                        getWaypoints(xmlJSONObj));
                allRoutes.add(route);
            }

            FileOutputStream f = new FileOutputStream(StaticVars.routesOutputSource);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(allRoutes);
//            allRoutes.forEach(route -> System.out.println(route.getName()));
            System.out.println("The routes have been saved successfully!");
            o.flush();
            o.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<Waypoint> getWaypoints(JSONObject obj){
        ArrayList<Waypoint> wps = new ArrayList<>();

        JSONArray wObj = null;
        if (obj.getJSONObject("route").getJSONObject("waypoints").getJSONArray("waypoint")!=null){
            wObj = obj.getJSONObject("route").getJSONObject("waypoints").getJSONArray("waypoint");

        }
        for (int i = 0; i < wObj.length();i++){
            wps.add(new Waypoint(wObj.getJSONObject(i).getJSONObject("position").getDouble("lat"),wObj.getJSONObject(i).getJSONObject("position").getDouble("lon")));
        }
        return  wps;
    }

    public static List<String> getRtzFilePaths(String dir, int depth) throws IOException {
        Pattern pattern = Pattern.compile(".*\\.rtz$");
        try (Stream<Path> stream = Files.walk(Paths.get(dir), depth)) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::toString)
                    .filter(pattern.asPredicate())
                    .collect(Collectors.toList());
        }
    }
}
