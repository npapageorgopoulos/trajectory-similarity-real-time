package routeconversion;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
public class Waypoint implements Serializable {
    private Double latitude;
    private Double longitude;


}
