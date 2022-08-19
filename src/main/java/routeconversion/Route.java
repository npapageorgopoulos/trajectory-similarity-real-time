package routeconversion;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;

@Getter
@Setter
@AllArgsConstructor
public class Route implements Serializable {

    private String name;
    private String validityPeriodStart;
    private String validityPeriodStop;
    private String vesselName;
    private String vesselVoyage;
    private ArrayList<Waypoint> route;


}
