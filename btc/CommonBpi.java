package btc;


import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class CommonBpi {

    private HashMap<String, Double> bpi_values  = new HashMap<String, Double>();

    public void setBpiValue(String time, Double value) {
        this.bpi_values.put(time, value);
        System.out.println(this.bpi_values);
    }

    public Double getBpiValue(String time) {
        System.out.println(this.bpi_values);
        return this.bpi_values.get(time);
    }


    public Double getAverageHourlyValue(String time_hour) {
        Double agg = 0.;
        int i = 0;
        for (Map.Entry<String, Double> values : this.bpi_values.entrySet()) {
            if ((new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00+00:00").format(values.getKey())).equals(time_hour)) {
                i++;
                agg = agg + values.getValue();
            }
        }
        return (agg/i);
    }




}

