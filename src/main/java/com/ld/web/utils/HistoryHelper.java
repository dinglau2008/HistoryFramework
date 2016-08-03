package com.ld.web.utils;

import com.ld.web.alg.YMDIndex;
import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by liuding on 8/3/16.
 */
public class HistoryHelper {

    private HistoryHelper(){}

    public static YMDIndex getYMDIndex(Date date)
    {
        if(date == null)
        {
            return null;
        }
        YMDIndex  ymd = new YMDIndex();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        ymd.setYear(calendar.get(Calendar.YEAR));
        ymd.setMonth(calendar.get(Calendar.MONTH));
        ymd.setDay(calendar.get(Calendar.DATE) - 1);

        return ymd;
    }

    public static Date buildDateFromIndex(int yearIndex, int monthIndex, int dayIndex, int epochYear )
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(epochYear+yearIndex, monthIndex, dayIndex+1);

        return DateUtils.truncate(calendar.getTime(), Calendar.DATE);
    }

}
