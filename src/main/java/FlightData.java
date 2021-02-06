public class FlightData {
//    {"ORIGIN_COUNTRY_NAME":"Romania","DEST_COUNTRY_NAME":"United States","count":1}
    public String origin_country_name;
    public String dest_country_name;
    public int count;

    public String getOrigin_country_name() {
        return origin_country_name;
    }

    public String getDest_country_name() {
        return dest_country_name;
    }

    public int getCount() {
        return count;
    }

    public void setOrigin_country_name(String origin_country_name) {
        this.origin_country_name = origin_country_name;
    }

    public void setDest_country_name(String dest_country_name) {
        this.dest_country_name = dest_country_name;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
