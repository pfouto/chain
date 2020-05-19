package frontend.utils;

import babel.events.InternalEvent;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EventPriorityQueue implements BlockingQueue<InternalEvent> {

    private static final InternalEvent.EventType[] DEFAULT_PRIORITY = {
            InternalEvent.EventType.TIMER_EVENT,
            InternalEvent.EventType.NOTIFICATION_EVENT,
            InternalEvent.EventType.MESSAGE_IN_EVENT,
            InternalEvent.EventType.MESSAGE_FAILED_EVENT,
            InternalEvent.EventType.MESSAGE_SENT_EVENT,
            InternalEvent.EventType.IPC_EVENT,
            InternalEvent.EventType.CUSTOM_CHANNEL_EVENT,
    };


    private Lock lock = new ReentrantLock();
    private Condition notEmpty = lock.newCondition();

    private InternalEvent.EventType[] priority;
    private Queue<InternalEvent>[] events;
    private int size;

    public EventPriorityQueue() {
        this(DEFAULT_PRIORITY);
    }

    @SuppressWarnings("unchecked")
    public EventPriorityQueue(InternalEvent.EventType[] priority) {
        this.priority = priority;
        this.events = new Queue[this.priority.length];
        for (int i = 0; i < this.events.length; i++)
            this.events[i] = new LinkedList<>();
        size = 0;
    }

    @Override
    public boolean add(InternalEvent protocolEvent) {
        boolean retVal = false;
        lock.lock();
        try {
            for (int i = 0; i < events.length; i++) {
                if (protocolEvent.getType() == priority[i]) {
                    retVal = events[i].add(protocolEvent);
                    this.size++;
                    notEmpty.signal();
                    return retVal;
                }
            }
        } finally {
            lock.unlock();
        }

        return retVal;
    }

    @Override
    public boolean offer(InternalEvent protocolEvent) {
        return false;
    }

    @Override
    public InternalEvent remove() {
        return null;
    }

    @Override
    public InternalEvent poll() {
        return null;
    }

    @Override
    public InternalEvent element() {
        return null;
    }

    @Override
    public InternalEvent peek() {
        return null;
    }

    @Override
    public void put(InternalEvent protocolEvent) throws InterruptedException {

    }

    @Override
    public boolean offer(InternalEvent protocolEvent, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public InternalEvent take() throws InterruptedException {
        lock.lock();
        try {
            while (size == 0)
                notEmpty.await();
            for (Queue<InternalEvent> event : events) {
                if (!event.isEmpty()) {
                    size--;
                    return event.poll();
                }
            }

        } finally {
            lock.unlock();
        }
        //will never happen?
        return null;
    }

    @Override
    public InternalEvent poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends InternalEvent> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<InternalEvent> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public int drainTo(Collection<? super InternalEvent> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super InternalEvent> c, int maxElements) {
        return 0;
    }
}
