package com.ld.web.alg;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Created by liuding on 8/3/16.
 */
public abstract class AbstractHistory<Key extends Comparable<Key>, Value> {

    public abstract Key ceilingKey(Key key);
    public abstract Key floorKey(Key key);
    public abstract List<Value> subMapValues(Key fromKey, boolean fromInclusive,
                                             Key toKey, boolean toInclusive);

    public abstract Value getValue(Key key);

    protected List<Value> valueList(NavigableMap<Key,Value> map)
    {
        List<Value> list = new ArrayList<Value>(map.size());
        if(map!=null){
            for(Key key:map.keySet()){
                list.add(map.get(key));
            }
        }
        return list;
    }

    public abstract Key getStartHisDateInPeriod(Key start, Key end);

    public abstract Set<Key> getKeySet(Key start, Key end);

    public void insert(Key key, Value value)
    {
        //default do nothing
        return;
    }

    public abstract Value merge(Key key, Value value,BiFunction<? super Value, ? super Value, ? extends Value> remappingFunction);

    public abstract List<DateValuePair> subMapPair(Key fromKey, boolean fromInclusive,
                                                   Key toKey, boolean toInclusive);


    public abstract boolean isEmpty();
}
