package com.ld.web.alg;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.time.DateUtils;

import java.io.*;
import java.util.*;

/**
 * Created by liuding on 8/3/16.
 */
public class CompressedHistoryTest {

    private static Date epoch;
    private static BufferedWriter bw ;

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2014, 0, 1);
        epoch = DateUtils.truncate(calendar.getTime(), calendar.DATE);
    }

    public static void validateData() throws IOException {
        CompressedHistory<Integer> history = new CompressedHistory();
        history.init(epoch);
        TreeMap<Date, Integer> treeHistory = new TreeMap<>();


        insert(history, treeHistory, 1000);
        merge(history, treeHistory, 1000);

        validateGet(history, treeHistory);
        validateCellingAndFloor(history, treeHistory);
        validateSubMap(history, treeHistory);

        validateGetStartHisDateInPeriod(history, treeHistory);
    }




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

    private static void insert(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory, int maxValue) throws IOException {
        Random random = new Random();

        for(int i = 0; i < maxValue; i ++)
        {
            Date date = generateDate();
            Integer value = random.nextInt(1000000);
            bw.write(date + "\t" + value + "\n");


            history.insert(date, value);
            treeHistory.put(date, value);
        }


        System.out.println("insert done");

    }



    private static void merge(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory, int maxValue) throws IOException {
        Random random = new Random();

        for(int i = 0; i < maxValue; i ++)
        {
            Date date = generateDate();
            Integer value = random.nextInt(1000000);
            bw.write(date + "\t" + value + "\n");

            history.merge(date, value, Integer::sum);
            treeHistory.merge(date, value, Integer::sum);
        }


        System.out.println("merge done");

    }


    private static boolean isSameDay(Date date1, Date date2)
    {

        if(date1 == null && date2 == null)
        {
            return true;
        }

        return (date1.getTime() == date2.getTime());

    }


    public static Date getStartHisDateInPeriod(TreeMap<Date, Integer> treeHistory, Date start, Date end) {
        Date startDate = treeHistory.ceilingKey(start);
        Date endDate = treeHistory.floorKey(end);
        if ((startDate != null) && (endDate !=null) && (startDate.getTime()<=endDate.getTime()))
            return startDate;
        else
            return null;
    }


    public static void validateGetStartHisDateInPeriod(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory)
    {
        for(int i = 0; i < 10000; i ++)
        {
            Date date = generateDate();

            int span = i%100;
            Date endDate = DateUtils.addDays(date, span);


            Random random = new Random();
            boolean fi = random.nextBoolean();
            boolean ti = random.nextBoolean();

            Date treeDate = getStartHisDateInPeriod(treeHistory, date, endDate);
            Date firstDate = history.getStartHisDateInPeriod(date, endDate);

            if(!isSameDay(treeDate, firstDate))
            {
                System.out.println("should not happen");
            }

        }

        System.out.println("validateGetStartHisDateInPeriod done");
    }

    private static void validateValue(NavigableMap<Date,Integer> treeValues, List<Integer> values)
    {
        if(treeValues.isEmpty() && CollectionUtils.isEmpty(values))
        {
            return;
        }

        int j = 0;
        for(Date tmpDate : treeValues.keySet())
        {
            Integer treeValue = treeValues.get(tmpDate);
            Integer value = values.get(j);
            j++;

            if(!isEqual(value, treeValue))
            {
                System.out.println("should not happen");
            }
        }

    }


    public static void validateSubMap(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory)
    {
        for(int i = 0; i < 10000; i ++)
        {
            Date date = generateDate();

            int span = i%100;
            Date endDate = DateUtils.addDays(date, span);


            Random random = new Random();
            boolean fi = random.nextBoolean();
            boolean ti = random.nextBoolean();



            NavigableMap<Date,Integer> treeValues = treeHistory.subMap(date, fi, endDate, ti);
            List<Integer> values = history.subMapValues(date, fi, endDate, ti);

            treeValues = treeHistory.subMap(date, true, endDate, true);
            Set<Date> dateSet = history.getKeySet(date, endDate);

            for(Date dateKey : treeValues.keySet())
            {
                if(!dateSet.contains(dateKey))
                {
                    System.out.println("should not happen");
                }
            }


        }

        System.out.println("validateSubMap done");
    }


    public static void validateCellingAndFloor(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory)
    {

        for(int i = 0; i < 10000; i ++)
        {
            Date date = generateDate();

            Date treeCeilingKey = treeHistory.ceilingKey(date);
            Date ceilingKey = history.ceilingKey(date);

            if(!isSameDay(treeCeilingKey, ceilingKey))
            {
                System.out.println("should not happen");
                ceilingKey = history.ceilingKey(date);
            }

            Date treeFloorKey = treeHistory.floorKey(date);
            Date floorKey = history.floorKey(date);


            if(!isSameDay(treeFloorKey, floorKey))
            {
                System.out.println("should not happen");
                floorKey = history.floorKey(date);
            }
        }

        System.out.println("validateCellingAndFloor done");
    }

    public static boolean isEqual(Integer oldv, Integer newv)
    {
        if(oldv == null && newv == null)
        {
            return true;
        }

        if(oldv != null && newv != null)
        {
            return (oldv.compareTo(newv) == 0);
        }

        return false;
    }


    public static void validateGet(CompressedHistory<Integer> history, TreeMap<Date, Integer> treeHistory)
    {

        for(int i = 0; i < 1000; i ++)
        {
            Date date = generateDate();

            Integer treeValue = treeHistory.get(date);
            Integer value = history.getValue(date);

            if(!isEqual(value, treeValue))
            {
                System.out.println("should not happen");
                value = history.getValue(date);
            }
        }

        System.out.println("validateGet done");
    }


    public static void main(String[] args) throws IOException {
        bw = new BufferedWriter(new FileWriter("/tmp/compressed"));
        validateData();
        bw.close();
    }
}
