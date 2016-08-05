package com.ld.web.alg;

import java.util.Date;

/**
 * Created by liuding on 8/4/16.
 */
public class DateValuePair<T> {
    private Date date;
    private T value;

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
