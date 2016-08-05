package com.ld.web.alg;

/**
 * Created by liuding on 8/3/16.
 */
public class YMDIndex implements Comparable<YMDIndex> {
    public static final int MONTH_COUNT = 12;
    public static final int DAY_COUNT_PERMONTH = 31;
    int year;
    int month;
    int day;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void toNextDay()
    {
        if(day + 1 >= DAY_COUNT_PERMONTH)
        {
            toNextMonth();
        }
        else
        {
            day++;
        }
    }


    public void toLastDay()
    {
        if(day <= 0)
        {
            toLastMonth();
        }
        else
        {
            day--;
        }
    }


    public void toNextMonth()
    {
        if(month + 1 >= MONTH_COUNT)
        {
            toNextYear();
        }
        else
        {
            month++;
            day = 0;
        }
    }

    public void toLastMonth()
    {
        if(month <=0)
        {
            toLastYear();
        }
        else
        {
            month--;
            day = DAY_COUNT_PERMONTH-1;
        }
    }


    public void toNextYear()
    {
        year++;
        month = 0;
        day = 0;
    }


    public void toLastYear()
    {
        year--;
        month = MONTH_COUNT-1;
        day = DAY_COUNT_PERMONTH-1;
    }

    public void assign(YMDIndex index)
    {
        this.year = index.year;
        this.month = index.month;
        this.day = index.day;
    }


    @Override
    public int compareTo(YMDIndex o) {
        int yearDiff = year - o.year;

        if(yearDiff == 0)
        {
            int monthDiff = month - o.month;
            if(monthDiff == 0)
            {
                return (day - o.day);
            }
            return monthDiff;
        }

        return yearDiff;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YMDIndex ymdIndex = (YMDIndex) o;

        if (year != ymdIndex.year) return false;
        if (month != ymdIndex.month) return false;
        return day == ymdIndex.day;

    }

    @Override
    public int hashCode() {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + day;
        return result;
    }

    @Override
    public String toString() {
        return "YMDIndex{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                '}';
    }
}
