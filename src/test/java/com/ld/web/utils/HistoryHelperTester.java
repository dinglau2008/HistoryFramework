package com.ld.web.utils;

import com.ld.web.alg.YMDIndex;
import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * Created by liuding on 8/5/16.
 */
public class HistoryHelperTester {


    private static Date generateDate()
    {
        Random random = new Random();

        int year = random.nextInt(6) + 2015;
        int month = random.nextInt(12);
        int day = random.nextInt(28);

        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month+1, day+1);

        Date date = DateUtils.truncate(calendar.getTime(), calendar.DATE);

        if(date.getDate() > 28)
        {
            System.out.println("this should not happen");
        }

        return date;
    }


    public static void testGetYMDIndex()
    {
        for(int i = 0; i < 100000; i++)
        {
            Date date = generateDate();

            YMDIndex index = HistoryHelper.getYMDIndex(date);

            YMDIndex ymd = new YMDIndex();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);

            ymd.setYear(calendar.get(Calendar.YEAR));
            ymd.setMonth(calendar.get(Calendar.MONTH));
            ymd.setDay(calendar.get(Calendar.DATE) - 1);


            if(!index.equals(ymd))
            {
                System.out.println("what is the matter");
            }
        }
    }

    public static void main(String[] args) {
        testGetYMDIndex();
    }
}
