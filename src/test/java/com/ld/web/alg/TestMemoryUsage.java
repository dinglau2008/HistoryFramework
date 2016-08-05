package com.ld.web.alg;

import org.apache.commons.lang.time.DateUtils;

import java.util.*;

/**
 * Created by liuding on 8/4/16.
 */
public class TestMemoryUsage {

    private static Date epoch;
    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(1900, 0, 1);
        epoch = DateUtils.truncate(calendar.getTime(), calendar.DATE);
    }


    private static Date generateDate()
    {
        Random random = new Random();

        int year = random.nextInt(100) + 2000;
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


    private static void insert(CompressedHistoryTmp<Integer> history)
    {
        Random random = new Random();

        for(int i = 0; i < 1000; i ++)
        {
            Date date = generateDate();
            Integer value = random.nextInt(100000);
            history.insert(date, value);
        }



    }

    private static void insert(CompressedHistory<Integer> history)
    {
        Random random = new Random();

        for(int i = 0; i < 1000; i ++)
        {
            Date date = generateDate();
            Integer value = random.nextInt(100000);
            history.insert(date, value);
        }



    }


    public static void validateCompressedData()
    {
        long start = System.currentTimeMillis();
        List<CompressedHistoryTmp> historyList = new ArrayList<>();
        for(int i = 0; i < 100000; i++)
        {
            CompressedHistoryTmp<Integer> history = new CompressedHistoryTmp();
            historyList.add(history);
            history.init(epoch);
            insert(history);
            if(i%100 == 1)
            {
                long span = System.currentTimeMillis() - start;
                System.out.println("history " + i + " time + " + span/1000);
            }

        }


    }



    public static void validateOldCompressedData()
    {
        long start = System.currentTimeMillis();
        List<CompressedHistory> historyList = new ArrayList<>();
        for(int i = 0; i < 100000; i++)
        {
            CompressedHistory<Integer> history = new CompressedHistory();
            historyList.add(history);
            history.init(epoch);
            insert(history);
            if(i%100 == 1)
            {
                long span = System.currentTimeMillis() - start;
                System.out.println("history " + i + " time + " + span/1000);
            }

        }


    }
    public static void main(String[] args) {

        validateCompressedData();
    }
}
