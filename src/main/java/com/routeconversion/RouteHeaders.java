package com.routeconversion;

public enum RouteHeaders {
    ROUTE_NAME("route_name"),
    WAYPOINT_ID("waypoint_id"),
    LATITUDE("latitude"),
    LONGTITUDE("longitude");


    private String val;
    RouteHeaders(String val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return val;
    }
}
