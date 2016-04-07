package org.apache.cassandra.cache;

import java.util.Iterator;

interface EvictionStrategy extends CacheSize
{
    // Cache owns mapping. Strategy provides access tracking and eviction

    // process for new element:
    // lookup map, if not found use elementFor, add to map, casValue
    // in any case, access
    // if add to map fails, discard element (not calling remove)
    Entry elementFor(Object key, EntryOwner owner);

    void maybeEvict();
    void clear();

    // strategy reserves space, thus evicts (and possibly calls remove)
    // during Entry.casValue

    enum Specials
    {
        EMPTY,
        DISCARDED
    };

    public static boolean isSpecial(Object v)
    {
        return v == Specials.EMPTY || v == Specials.DISCARDED;
    }

    interface Entry
    {
        Object key();
        // Could be a Specials value.
        Object value(); 

        // It is an error to call this with old == DISCARDED or v == EMPTY or DISCARDED
        // Entails access
        boolean casValue(Object old, Object v);

        // Sets value to EMPTY. Returns old value on success.
        Object remove();

        void access();
    }

    interface EntryOwner
    {
        void evict(Entry entry);

        // entry's value is DISCARDED
        boolean removeMapping(Entry entry);
    }

    interface Weigher
    {
        long weigh(Object key, Object value);
    }
}