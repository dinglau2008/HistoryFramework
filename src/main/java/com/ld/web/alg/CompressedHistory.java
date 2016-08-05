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
 * Created by liuding on 8/3/16.
 */
public class CompressedHistory<T> extends AbstractHistory<Date, T> {
    private static final int MAX_FUTURE_YEAR = 5;
    private static final short INVALIDE_INDEX = -1;
    private static final int INIT_HISTORY_VALUE_SIZE = 16;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private YMDIndex epochIndex;
    private Date earliestDate = null;
    volatile YMDIndex earliestIndex;
    private YMDIndex latestIndex;

    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    short [][][] valueDateIndex;

    private volatile T[] historyValues = (T[]) new Object[INIT_HISTORY_VALUE_SIZE];
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

        valueDateIndex = new short[yearSpan][][];
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
        historyValues = (T[]) new Object[INIT_HISTORY_VALUE_SIZE];
        historySize = INIT_HISTORY_VALUE_SIZE;
        valueSize = 0;
    }


    private void allocateMemory()
    {
        int newSize =historySize<<1;
        T[] newValues = (T[]) new Object[newSize];

        for(int i = 0; i < historySize; i++)
        {
            newValues[i] = historyValues[i];
        }

        historyValues = newValues;
        historySize = newSize;
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
                valueDateIndex[yearIndex] = new short[YMDIndex.MONTH_COUNT][];
            }

            if(valueDateIndex[yearIndex][dateIndex.month] == null)
            {
                valueDateIndex[yearIndex][dateIndex.month] = new short[YMDIndex.DAY_COUNT_PERMONTH];
                for(int i = 0; i < YMDIndex.DAY_COUNT_PERMONTH; i++)
                {
                    valueDateIndex[yearIndex][dateIndex.month][i] = INVALIDE_INDEX;
                }
            }

            int index = valueDateIndex[yearIndex][dateIndex.month][dateIndex.day];
            if(index == INVALIDE_INDEX)
            {
                if(valueSize >= historySize)
                {
                    allocateMemory();
                }
                historyValues[valueSize] = value;
                valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] = valueSize;
                valueSize++;

            }
            else
            {
                historyValues[index] = value;
            }
        }
        finally {
            wLock.unlock();
        }
    }




    @Override
    public T merge(Date date, T newValue, BiFunction<? super T, ? super T, ? extends T> remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        Objects.requireNonNull(newValue);


        if(date == null)
        {
            logger.warn("event date should not be null");
            return null;
        }
        YMDIndex dateIndex = HistoryHelper.getYMDIndex(date);

        if(dateIndex.compareTo(epochIndex) < 0 || dateIndex.compareTo(latestIndex) > 0)
        {
            logger.warn("Event happen on {}  out of permitted period",  date);
            return null;
        }

        int yearIndex = dateIndex.getYear() - epochIndex.getYear();

        T result = newValue;

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
                valueDateIndex[yearIndex] = new short[YMDIndex.MONTH_COUNT][];
            }

            if(valueDateIndex[yearIndex][dateIndex.month] == null)
            {
                valueDateIndex[yearIndex][dateIndex.month] = new short[YMDIndex.DAY_COUNT_PERMONTH];
                for(int i = 0; i < YMDIndex.DAY_COUNT_PERMONTH; i++)
                {
                    valueDateIndex[yearIndex][dateIndex.month][i] = INVALIDE_INDEX;
                }
            }

            int index = valueDateIndex[yearIndex][dateIndex.month][dateIndex.day];
            if(index == INVALIDE_INDEX)
            {
                if(valueSize >= historySize)
                {
                    allocateMemory();
                }
                historyValues[valueSize] = newValue;
                valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] = valueSize;
                valueSize++;

            }
            else
            {
                if(remappingFunction == null)
                {
                    historyValues[index] = newValue;
                }
                else
                {
                    result = remappingFunction.apply(historyValues[index], newValue);
                    historyValues[index] = result;
                }
            }
        }
        finally {
            wLock.unlock();
        }

        return result;
    }

    @Override
    public List<DateValuePair> subMapPair(Date fromKey, boolean fromInclusive, Date toKey, boolean toInclusive) {

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

            List<DateValuePair> pairList = new ArrayList<DateValuePair>();


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
                                short index = valueDateIndex[yearIndex][fromIndex.month][fromIndex.day];
                                if(index != INVALIDE_INDEX)
                                {
                                    DateValuePair<T> dateValuePair = new DateValuePair<>();
                                    dateValuePair.setDate(HistoryHelper.buildDateFromIndex(fromIndex));
                                    dateValuePair.setValue(this.historyValues[index]);
                                    pairList.add(dateValuePair);
                                }
                                fromIndex.day++;
                                if(fromIndex.compareTo(toIndex) > 0)
                                {
                                    return pairList;
                                }
                            }
                        }
                        fromIndex.month++;
                        fromIndex.day = 0;
                        if(fromIndex.compareTo(toIndex) > 0)
                        {
                            return pairList;
                        }
                    }
                }
                fromIndex.toNextYear();
            }

            return pairList;

        }
        finally {
            rLock.unlock();
        }

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
                int yearIndex = dateIndex.getYear() - epochIndex.year;

                if(valueDateIndex[yearIndex] != null)
                {
                    while(dateIndex.month < YMDIndex.MONTH_COUNT)
                    {
                        if(valueDateIndex[yearIndex][dateIndex.month] != null)
                        {
                            while(dateIndex.day < YMDIndex.DAY_COUNT_PERMONTH)
                            {
                                if(valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] != INVALIDE_INDEX)
                                {
                                    return HistoryHelper.buildDateFromIndex(dateIndex);
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
                                if(valueDateIndex[yearIndex][dateIndex.month][dateIndex.day] != INVALIDE_INDEX)
                                {
                                    return HistoryHelper.buildDateFromIndex(dateIndex);
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



    @Override
    public List<T> subMapValues(Date fromKey, boolean fromInclusive, Date toKey, boolean toInclusive) {

        List<DateValuePair> pairList = subMapPair(fromKey, fromInclusive, toKey, toInclusive);

        if(CollectionUtils.isEmpty(pairList))
        {
            return null;
        }

        List<T> values = new ArrayList<>();
        for(DateValuePair<T> pair : pairList)
        {
            values.add(pair.getValue());
        }
        return values;
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
        if(valueDateIndex[yearIndex][monthIndex][dayIndex] == INVALIDE_INDEX)
        {
            return null;
        }

        return historyValues[valueDateIndex[yearIndex][monthIndex][dayIndex]];
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
                                if(valueDateIndex[yearIndex][fromIndex.month][fromIndex.day] != INVALIDE_INDEX)
                                {

                                    return HistoryHelper.buildDateFromIndex(fromIndex);
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


        List<DateValuePair> pairList = subMapPair(start, true, end, true);

        if(CollectionUtils.isEmpty(pairList))
        {
            return null;
        }


        Set<Date> dateList = new HashSet<>();
        for(DateValuePair<T> pair : pairList)
        {
            dateList.add(pair.getDate());
        }
        return dateList;

    }


    @Override
    public boolean isEmpty(){
        return earliestDate==null;
    }
}
