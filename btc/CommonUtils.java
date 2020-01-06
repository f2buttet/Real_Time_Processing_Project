package btc;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CommonUtils {

    public String addHoursToString (String time, int hours, String formated_time) {
        String formatedTime = "";
        try {
            Date parsedTime = new SimpleDateFormat(formated_time).parse(time);
            formatedTime = new SimpleDateFormat(formated_time).format(addHoursToJavaUtilDate(parsedTime, hours));
        }  catch (Exception e) {
            System.out.println("parse Exception :" + e);
        }
        return formatedTime;
    }

    public Date addHoursToJavaUtilDate(Date date, int hours) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, hours);
        return calendar.getTime();
    }
}
