package com.ld.web.utils;

import com.ld.web.alg.YMDIndex;
import org.apache.commons.lang.time.DateUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liuding on 8/3/16.
 */
public class HistoryHelper {

    private HistoryHelper(){}
    private static Map<Long, YMDIndex> time2Index = new HashMap<>();
    private static final long MILLSECONDS_PER_DAY = 24*3600*1000;
    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();
    private static final long LOCAL_TIME_OFFSET = LOCAL_TIME_ZONE.getRawOffset();

    private static long quickTruncate2Date(Date date)
    {
        long time = date.getTime();
        long dayTime = (time+LOCAL_TIME_OFFSET)/MILLSECONDS_PER_DAY;
        return dayTime;

    }

    public static YMDIndex getYMDIndex(Date date)
    {
        if(date == null)
        {
            return null;
        }

        long dateTime = quickTruncate2Date(date);

        YMDIndex result = new YMDIndex();
        YMDIndex  ymd = time2Index.get(dateTime);

        if(ymd == null)
        {
            ymd = new YMDIndex();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);

            ymd.setYear(calendar.get(Calendar.YEAR));
            ymd.setMonth(calendar.get(Calendar.MONTH));
            ymd.setDay(calendar.get(Calendar.DATE) - 1);
            time2Index.put(dateTime, ymd);
        }

        result.assign(ymd);
        return result;
    }

    public static Date buildDateFromIndex(int yearIndex, int monthIndex, int dayIndex, int epochYear )
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(epochYear+yearIndex, monthIndex, dayIndex+1);

        return DateUtils.truncate(calendar.getTime(), Calendar.DATE);
    }

}
