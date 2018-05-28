package toolpool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SimpleDT {
    private static final String date_format = "yyyy-MM-dd HH:mm:ss";
    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>();

    private static final String date_format1 = "dd/MM/yyyy HH:mm:ss";
    private static ThreadLocal<DateFormat> threadLocal1 = new ThreadLocal<DateFormat>();
    public static DateFormat getDateFormat()
    {
        DateFormat df = threadLocal.get();
        if(df==null){
            df = new SimpleDateFormat(date_format);
            threadLocal.set(df);
        }
        return df;
    }

    public static String formatDate(Date date) throws ParseException {
        return getDateFormat().format(date);
    }

    public static Date parse(String strDate) throws ParseException {
        return getDateFormat().parse(strDate);
    }

    public static DateFormat getDateFormat1()
    {
        DateFormat df = threadLocal1.get();
        if(df==null){
            df = new SimpleDateFormat(date_format1);
            threadLocal1.set(df);
        }
        return df;
    }

    public static Date parse1(String strDate) throws ParseException {
        return getDateFormat1().parse(strDate);
    }
}
