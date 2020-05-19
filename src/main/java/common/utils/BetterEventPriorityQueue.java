package common.utils;

import babel.events.InternalEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class BetterEventPriorityQueue implements BlockingQueue<InternalEvent> {

    private static final InternalEvent.EventType[] DEFAULT_PRIORITY = {
            InternalEvent.EventType.TIMER_EVENT,
            InternalEvent.EventType.NOTIFICATION_EVENT,
            InternalEvent.EventType.MESSAGE_IN_EVENT,
            InternalEvent.EventType.MESSAGE_FAILED_EVENT,
            InternalEvent.EventType.MESSAGE_SENT_EVENT,
            InternalEvent.EventType.IPC_EVENT,
            InternalEvent.EventType.CUSTOM_CHANNEL_EVENT,
    };

    private final Semaphore semaphore = new Semaphore(0);

    private final InternalEvent.EventType[] priority;
    private final BlockingQueue<InternalEvent>[] events;

    public BetterEventPriorityQueue() {
        this(DEFAULT_PRIORITY);
    }

    @SuppressWarnings("unchecked")
    public BetterEventPriorityQueue(InternalEvent.EventType[] priority) {
        this.priority = priority;
        this.events = new BlockingQueue[this.priority.length];
        for (int i = 0; i < this.events.length; i++)
            this.events[i] = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean add(InternalEvent protocolEvent) {
        for (int i = 0; i < events.length; i++) {
            if (protocolEvent.getType() == priority[i]) {
                events[i].add(protocolEvent);
                semaphore.release();
                return true;
            }
        }
        throw new IllegalStateException("Event type not supported: " + protocolEvent);
    }

    @Override
    public InternalEvent take() throws InterruptedException {
        semaphore.acquire();

        InternalEvent taken = null;
        while (taken == null) {
            for (Queue<InternalEvent> event : events) {
                if (!event.isEmpty()) {
                    taken = event.poll();
                    if (taken != null)
                        break;
                }
            }
        }
        return taken;

    }

    @Override
    public boolean offer(InternalEvent protocolEvent) {
        throw new IllegalStateException();
    }

    @Override
    public InternalEvent remove() {
        throw new IllegalStateException();
    }

    @Override
    public InternalEvent poll() {
        throw new IllegalStateException();
    }

    @Override
    public InternalEvent element() {
        throw new IllegalStateException();
    }

    @Override
    public InternalEvent peek() {
        throw new IllegalStateException();
    }

    @Override
    public void put(InternalEvent protocolEvent) throws InterruptedException {
        throw new IllegalStateException();

    }

    @Override
    public boolean offer(InternalEvent protocolEvent, long timeout, TimeUnit unit) throws InterruptedException {
        throw new IllegalStateException();
    }

    @Override
    public InternalEvent poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new IllegalStateException();
    }

    @Override
    public int remainingCapacity() {
        throw new IllegalStateException();
    }

    @Override
    public boolean remove(Object o) {
        throw new IllegalStateException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new IllegalStateException();
    }

    @Override
    public boolean addAll(Collection<? extends InternalEvent> c) {
        throw new IllegalStateException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new IllegalStateException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new IllegalStateException();
    }

    @Override
    public void clear() {
        throw new IllegalStateException();

    }

    @Override
    public int size() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isEmpty() {
        return semaphore.availablePermits() == 0;
    }

    @Override
    public boolean contains(Object o) {
        throw new IllegalStateException();
    }

    @Override
    public Iterator<InternalEvent> iterator() {
        throw new IllegalStateException();
    }

    @Override
    public Object[] toArray() {
        throw new IllegalStateException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new IllegalStateException();
    }

    @Override
    public int drainTo(Collection<? super InternalEvent> c) {
        throw new IllegalStateException();
    }

    @Override
    public int drainTo(Collection<? super InternalEvent> c, int maxElements) {
        throw new IllegalStateException();
    }
}
