package com.ld.web.alg;

import com.ld.web.utils.HistoryHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

/**
 * Created by liuding on 8/4/16.
 */
public class CompressedHistoryTmp<T> extends AbstractHistory<Date, T> {
    private static final int MAX_FUTURE_YEAR = 5;
    private static final int INIT_HISTORY_VALUE_SIZE = 16;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private YMDIndex epochIndex;
    private Date earliestDate = null;
    private volatile YMDIndex earliestIndex;
    private YMDIndex latestIndex;

    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private T [][][] valueDateIndex;

    private volatile int historySize = INIT_HISTORY_VALUE_SIZE;
    private volatile short valueSize = 0;


    public void init(Date epoch)
    {
        if(epoch == null)
        {
            logger.error("epoch should not be null");
            throw new NullPointerException("epoch should not be null");
        }

        epochIndex = HistoryHelper.getYMDIndex(epoch);

        YMDIndex nowIndex =  HistoryHelper.getYMDIndex(new Date());

        if(nowIndex.getYear() < epochIndex.getYear())
        {
            logger.error("epoch is too late {}", epoch);
            throw new  IllegalArgumentException("epoch should not be later than current year");
        }


        int yearSpan = nowIndex.getYear() - epochIndex.getYear() + MAX_FUTURE_YEAR + 1; //add 10 years as year buffer

        valueDateIndex = (T [][][])new Object[yearSpan][][];
        latestIndex = new YMDIndex();
        latestIndex.setMonth(YMDIndex.MONTH_COUNT-1);
        latestIndex.setDay(YMDIndex.DAY_COUNT_PERMONTH -1);
        latestIndex.setYear(nowIndex.year+MAX_FUTURE_YEAR);

    }

    public void destory()
    {
        epochIndex = null;
        earliestDate = null;
        earliestIndex = null;
        latestIndex = null;
        valueDateIndex = null;
        historySize = INIT_HISTORY_VALUE_SIZE;
        valueSize = 0;
    }



    public void insert(Date date , T value)
    {
        if(date == null)
        {
            logger.warn("event date should not be null");
            return;
        }
        YMDIndex dateIndex = HistoryHelper.getYMDIndex(date);

        if(dateIndex.compareTo(epochIndex) < 0 || dateIndex.compareTo(latestIndex) > 0)
        {
            logger.warn("Event happen on {}  out of permitted period",  date);
            return;
        }

        int yearIndex = dateIndex.getYear() - epochIndex.getYear();


        ReentrantReadWriteLock.WriteLock wLock = rwLock.writeLock();
        wLock.lock();

        try
        {
            if(earliestDate == null || date.before(earliestDate))
            {
                earliestDate = date;
                earliestIndex = HistoryHelper.getYMDIndex(date);
            }

            if(valueDateIndex[yearIndex] == null)
            {
                valueDateIndex[yearIndex] = (T [][])new Object[YMDIndex.MONTH_COUNT][];
            }

            if(valueDateIndex[yearIndex][dateIndex.month] == null)
            {
                valueDateIndex[yearIndex][dateIndex.month] = (T [])new Object[YMDIndex.DAY_COUNT_PERMONTH];
            }

            valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] = value;
        }
        finally {
            wLock.unlock();
        }
    }

    @Override
    public T merge(Date date, T t, BiFunction<? super T, ? super T, ? extends T> remappingFunction) {
        return null;
    }

    @Override
    public List<DateValuePair> subMapPair(Date fromKey, boolean fromInclusive, Date toKey, boolean toInclusive) {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }


    @Override
    public Date ceilingKey(Date date) {
        if(date == null)
        {
            logger.warn("event date should not be null");
            return null;
        }
        if(earliestIndex == null)
        {
            return null;
        }

        YMDIndex dateIndex = HistoryHelper.getYMDIndex(date);

        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {
            if(dateIndex.compareTo(earliestIndex) < 0)
            {
                dateIndex.assign(earliestIndex);
            }

            while(dateIndex.compareTo(latestIndex) <= 0)
            {
                int yearIndex = dateIndex.getYear() - epochIndex.getYear();

                if(valueDateIndex[yearIndex] != null)
                {
                    while(dateIndex.month < YMDIndex.MONTH_COUNT)
                    {
                        if(valueDateIndex[yearIndex][dateIndex.month] != null)
                        {
                            while(dateIndex.day < YMDIndex.DAY_COUNT_PERMONTH)
                            {
                                if(valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] != null)
                                {
                                    return HistoryHelper.buildDateFromIndex(yearIndex, dateIndex.month, dateIndex.day, epochIndex.year);
                                }
                                dateIndex.day++;
                            }
                        }
                        dateIndex.month++;
                        dateIndex.day = 0;
                    }
                }
                dateIndex.toNextYear();
            }
        }
        finally {
            rLock.unlock();
        }
        return null;
    }

    @Override
    public Date floorKey(Date date) {

        if(date == null)
        {
            logger.warn("event date should not be null");
            return null;
        }

        if(earliestIndex == null)
        {
            return null;
        }
        YMDIndex dateIndex = HistoryHelper.getYMDIndex(date);

        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {
            if(dateIndex.compareTo(latestIndex) > 0)
            {
                dateIndex.assign(latestIndex);
                return null;
            }

            while(dateIndex.compareTo(earliestIndex) >= 0)
            {
                int yearIndex = dateIndex.getYear() - epochIndex.getYear();

                if(valueDateIndex[yearIndex] != null)
                {
                    while(dateIndex.month >= 0)
                    {
                        if(valueDateIndex[yearIndex][dateIndex.month] != null)
                        {
                            while(dateIndex.day >=0 )
                            {
                                if(valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] != null)
                                {
                                    return HistoryHelper.buildDateFromIndex(yearIndex, dateIndex.month, dateIndex.day, epochIndex.year);
                                }
                                dateIndex.day--;
                            }
                        }
                        dateIndex.month--;
                        dateIndex.day = YMDIndex.DAY_COUNT_PERMONTH-1;
                    }
                }
                dateIndex.toLastYear();
            }
        }
        finally {
            rLock.unlock();
        }

        return null;
    }


    private List<Date> subMap(Date fromKey, boolean fromInclusive, Date toKey, boolean toInclusive)
    {
        if(earliestIndex == null)
        {
            return null;
        }
        if(fromKey == null || toKey == null)
        {
            logger.warn("date should not be numll, start {} end {}", fromKey, toKey);
            return null;
        }

        if(toKey.before(fromKey))
        {
            logger.warn("endDate {} shouldn't precede startDate {}", toKey, fromKey);
            return null;
        }

        Date fromDate = fromKey;
        if(!fromInclusive)
        {
            fromDate = DateUtils.addDays(fromKey, 1);
        }

        Date toDate = toKey;
        if(!toInclusive)
        {
            toDate = DateUtils.addDays(toKey, -1);
        }

        if(toDate.before(fromDate))
        {
            logger.warn("no period is allowed from " + fromKey + " " + fromInclusive
                    + " to " + toKey + " " + toInclusive);
            return null;
        }


        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {
            YMDIndex fromIndex = HistoryHelper.getYMDIndex(fromDate);
            YMDIndex toIndex = HistoryHelper.getYMDIndex(toDate);

            if(fromIndex.compareTo(earliestIndex) < 0)
            {
                fromIndex.assign(earliestIndex);
            }
            if(toIndex.compareTo(latestIndex) > 0 )
            {
                toIndex.assign(latestIndex);
            }

            List<Date> dateList = new ArrayList<Date>();


            while(fromIndex.compareTo(toIndex) <= 0)
            {
                int yearIndex = fromIndex.year - epochIndex.year;

                if(valueDateIndex[yearIndex] != null)
                {
                    while(fromIndex.month < YMDIndex.MONTH_COUNT)
                    {
                        if(valueDateIndex[yearIndex][fromIndex.month] != null)
                        {
                            while(fromIndex.day < YMDIndex.DAY_COUNT_PERMONTH)
                            {
                                if(valueDateIndex[yearIndex][fromIndex.month][fromIndex.day] != null)
                                {
                                    dateList.add(HistoryHelper.buildDateFromIndex(yearIndex, fromIndex.month, fromIndex.day, epochIndex.year));
                                }
                                fromIndex.day++;
                                if(fromIndex.compareTo(toIndex) > 0)
                                {
                                    return dateList;
                                }
                            }
                        }
                        fromIndex.month++;
                        fromIndex.day = 0;
                        if(fromIndex.compareTo(toIndex) > 0)
                        {
                            return dateList;
                        }
                    }
                }
                fromIndex.toNextYear();
            }

            return dateList;

        }
        finally {
            rLock.unlock();
        }


    }


    @Override
    public List<T> subMapValues(Date fromKey, boolean fromInclusive, Date toKey, boolean toInclusive) {

        if(fromKey == null || toKey == null)
        {
            logger.warn("date should not be numll, start {} end {}", fromKey, toKey);
            return null;
        }

        if(earliestIndex == null)
        {
            return null;
        }
        if(toKey.before(fromKey))
        {
            logger.warn("endDate {} shouldn't precede startDate {}", toKey, fromKey);
            return null;
        }

        Date fromDate = fromKey;
        if(!fromInclusive)
        {
            fromDate = DateUtils.addDays(fromKey, 1);
        }

        Date toDate = toKey;
        if(!toInclusive)
        {
            toDate = DateUtils.addDays(toKey, -1);
        }

        if(toDate.before(fromDate))
        {
            logger.warn("no period is allowed from " + fromKey + " " + fromInclusive
                    + " to " + toKey + " " + toInclusive);
            return null;
        }


        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {
            YMDIndex fromIndex = HistoryHelper.getYMDIndex(fromDate);
            YMDIndex toIndex = HistoryHelper.getYMDIndex(toDate);

            if(fromIndex.compareTo(earliestIndex) < 0)
            {
                fromIndex.assign(earliestIndex);
            }
            if(toIndex.compareTo(latestIndex) > 0 )
            {
                toIndex.assign(latestIndex);
            }

            List<T> valueList = new ArrayList<T>();


            while(fromIndex.compareTo(toIndex) <= 0)
            {
                int yearIndex = fromIndex.year - epochIndex.year;

                if(valueDateIndex[yearIndex] != null)
                {
                    while(fromIndex.month < YMDIndex.MONTH_COUNT)
                    {
                        if(valueDateIndex[yearIndex][fromIndex.month] != null)
                        {
                            while(fromIndex.day < YMDIndex.DAY_COUNT_PERMONTH)
                            {
                                if(valueDateIndex[yearIndex][fromIndex.month][fromIndex.day] != null)
                                {
                                    valueList.add(valueDateIndex[yearIndex][fromIndex.month][fromIndex.day]);
                                }
                                fromIndex.day++;
                                if(fromIndex.compareTo(toIndex) > 0)
                                {
                                    return valueList;
                                }
                            }
                        }
                        fromIndex.month++;
                        fromIndex.day = 0;
                        if(fromIndex.compareTo(toIndex) > 0)
                        {
                            return valueList;
                        }
                    }
                }
                fromIndex.toNextYear();
            }

            return valueList;

        }
        finally {
            rLock.unlock();
        }

    }


    private T getValueByIndex(int yearIndex, int monthIndex, int dayIndex)
    {
        if(valueDateIndex[yearIndex] == null)
        {
            return null;
        }
        if(valueDateIndex[yearIndex][monthIndex] == null)
        {
            return null;
        }
        if(valueDateIndex[yearIndex][monthIndex][dayIndex] == null)
        {
            return null;
        }

        return valueDateIndex[yearIndex][monthIndex][dayIndex];
    }


    @Override
    public T getValue(Date date) {
        if(date == null)
        {
            logger.warn("event date should not be null");
            return null;
        }
        if(earliestIndex == null)
        {
            return null;
        }

        YMDIndex dateIndex = HistoryHelper.getYMDIndex(date);


        if(dateIndex.compareTo(this.earliestIndex) < 0 || dateIndex.compareTo(latestIndex) > 0)
        {
            return null;
        }

        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {

            int yearIndex = dateIndex.getYear() - epochIndex.getYear();
            int monthIndex = dateIndex.month;
            int dayIndex = dateIndex.day;

            return getValueByIndex(yearIndex, monthIndex, dayIndex);
        }
        finally {
            rLock.unlock();
        }






    }

    @Override
    public Date getStartHisDateInPeriod(Date start, Date end) {

        if(start == null || end == null)
        {
            logger.warn("date should not be numll, start {} end {}", start, end);
            return null;
        }

        if(earliestIndex == null)
        {
            return null;
        }
        if(end.before(start))
        {
            logger.warn("endDate {} shouldn't precede startDate {}", end, start);
            return null;
        }

        YMDIndex fromIndex = HistoryHelper.getYMDIndex(start);
        YMDIndex toIndex = HistoryHelper.getYMDIndex(end);

        if(toIndex.compareTo(latestIndex) >=0 )
        {
            toIndex.assign(latestIndex);
        }

        if(fromIndex.compareTo(earliestIndex) <= 0 )
        {
            fromIndex.assign(earliestIndex);
        }


        ReentrantReadWriteLock.ReadLock rLock= rwLock.readLock();
        rLock.lock();

        try
        {

            while(fromIndex.compareTo(toIndex) <= 0)
            {
                int yearIndex = fromIndex.year - epochIndex.year;

                if(valueDateIndex[yearIndex] != null)
                {
                    while(fromIndex.month < YMDIndex.MONTH_COUNT)
                    {
                        if(valueDateIndex[yearIndex][fromIndex.month] != null)
                        {
                            while(fromIndex.day < YMDIndex.DAY_COUNT_PERMONTH)
                            {
                                if(valueDateIndex[yearIndex][fromIndex.month][fromIndex.day] != null)
                                {
                                    return HistoryHelper.buildDateFromIndex(yearIndex,
                                            fromIndex.month, fromIndex.day, epochIndex.year);
                                }
                                fromIndex.day++;
                                if(fromIndex.compareTo(toIndex) > 0)
                                {
                                    return null;
                                }
                            }
                        }
                        fromIndex.month++;
                        fromIndex.day = 0;

                        if(fromIndex.compareTo(toIndex) > 0)
                        {
                            return null;
                        }
                    }
                }
                fromIndex.toNextYear();
            }

            return null;
        }
        finally {
            rLock.unlock();
        }

    }


    @Override
    public Set<Date> getKeySet(Date start, Date end) {

        if(earliestIndex == null)
        {
            return null;
        }
        List<Date> dateList = this.subMap(start, true, end, true);

        if(CollectionUtils.isEmpty(dateList))
        {
            return null;
        }

        Set<Date> dateSet = new HashSet<>();

        dateSet.addAll(dateList);
        return dateSet;

    }
}
