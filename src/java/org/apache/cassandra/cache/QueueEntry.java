package org.apache.cassandra.cache;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class QueueEntry<Element>
{
    private AtomicReference<QueueEntry<Element>> next;
    private volatile Element content;     // set at construction, changes to null to mark deleted

    public QueueEntry(Element content)
    {
        this.next = new AtomicReference<>(null);
        this.content = content;
    }

    public Element content()
    {
        return content;     // could be null
    }

    public void delete()
    {
        assert content != null;
        content = null;

        discardNextDeleted();
    }

    public boolean deleted()
    {
        return content == null;
    }

    public QueueEntry<Element> addToQueue(QueueEntry<Element> queue)
    {
        do
        {
            QueueEntry<Element> next = queue.next.get();
            while (next != null)
            {
                queue = next;
                next = next.next.get();
            }
        }
        while (!queue.next.compareAndSet(null, this));
        return this;
    }

    public QueueEntry<Element> discardNextDeleted()
    {
        // Remove nexts while they don't have content, but make sure to point to a trailing entry to make sure we don't
        // skip over something that is just being added.
        QueueEntry<Element> next = this.next.get();
        if (next == null)
            return this;
        if (!next.deleted())
            return next;

        QueueEntry<Element> nextnext = next.next.get();
        if (nextnext == null)
            return next;        // still no change wanted

        do
        {
            next = nextnext;
            nextnext = next.next.get();
            if (nextnext == null)
                break;
        }
        while (next.deleted());

        assert next != null;
        this.next.lazySet(next);
        return next;
    }

    public Iterator<Element> iterator()
    {
        return new Iter();
    }

    public String toString()
    {
        return toString(new HashSet<>());
    }

    public String toString(Set<QueueEntry<Element>> s)
    {
        String r = (content != null ? content.toString() : "#");
        if (!s.add(this))
            return r + "*Loop*";
        QueueEntry<Element> next = this.next.get();
        int nc = 0;
        while (next != null && next.content == null)
        {
            next = next.next.get();
            ++nc;
        }
        if (nc > 0)
            r += "--" + nc + "-->";
        else if (next != null)
            r += "-->";

        if (next != null)
            if (s.size() < 100)
                r += next.toString(s);
            else
                r += "...";
        return r;
    }

    class Iter implements Iterator<Element>
    {
        QueueEntry<Element> qe = QueueEntry.this;

        @Override
        public boolean hasNext()
        {
            while (qe != null && qe.deleted())
                qe = qe.next.get();
            return qe != null;
        }

        @Override
        public Element next()
        {
            if (!hasNext())
                throw new AssertionError();
            Element content = qe.content();
            qe = qe.next.get();
            return content;
        }
    }
}