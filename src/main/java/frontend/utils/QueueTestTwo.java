package frontend.utils;

import pt.unl.fct.di.novasys.babel.internal.*;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class QueueTestTwo {

    static int N_PRODUCERS = 20;

    static ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
    static volatile long nEvents = 0;

    public static void main(String[] args) throws InterruptedException, UnknownHostException {


        System.out.println(Arrays.toString(InetAddress.getByName("localhost").getHostAddress().split("\\.")));
        System.exit(0);

        tmx.setThreadContentionMonitoringEnabled(true);

        BlockingQueue<Double> javaBlockingQueue = new SemaphoreQueue<>();

        Thread consumer = new Thread(() -> {
            while (true) {
                try {
                    Double take = javaBlockingQueue.take();
                    nEvents += 1;
                    for(int i = 0;i<1;i++){
                        take++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        List<Thread> producers = new LinkedList<>();
        for (int i = 0; i < N_PRODUCERS; i++) {
            producers.add(new Thread(() -> {
                while (true)
                    javaBlockingQueue.add(10d);
            }));
        }

        Thread monitor = new Thread(() -> {
            long lastBC = 0, lastBT = 0, lastWC = 0, lastWT = 0, lastET = 0;

            try {
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(10000);
                    ThreadInfo threadInfo = tmx.getThreadInfo(producers.get(0).getId());

                    long cBC = threadInfo.getBlockedCount();
                    long cBT = threadInfo.getBlockedTime();
                    long cWC = threadInfo.getWaitedCount();
                    long cWT = threadInfo.getWaitedTime();
                    long cET = nEvents;

                    System.out.println("bc " + (cBC - lastBC) + " bt " + (cBT - lastBT) +
                            " wc " + (cWC - lastWC) + " wt " + (cWT - lastWT) + " et " + (cET - lastET));

                    lastBC = cBC;
                    lastBT = cBT;
                    lastWC = cWC;
                    lastWT = cWT;
                    lastET = cET;


                }
                System.exit(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        consumer.start();
        producers.forEach(Thread::start);
        monitor.start();

    }

    public static class SemaphoreQueue<T> implements BlockingQueue<T> {

        static final Semaphore semaphore = new Semaphore(0);
        static final ReentrantLock addLock = new ReentrantLock();

        transient Node<T> head;
        private transient Node<T> last;

        public SemaphoreQueue(){
            last = head = new Node<T>(null);
        }

        @Override
        public boolean add(T t) {
            Node<T> node = new Node<>(t);
            synchronized (addLock){
                last = last.next = node;
            }
            semaphore.release();
            return true;
        }

        @Override
        public boolean offer(T t) {
            return false;
        }

        @Override
        public T remove() {
            return null;
        }

        @Override
        public T poll() {
            return null;
        }

        @Override
        public T element() {
            return null;
        }

        @Override
        public T peek() {
            return null;
        }

        @Override
        public void put(T t) throws InterruptedException {

        }

        @Override
        public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public T take() throws InterruptedException {
            semaphore.acquire();
            Node<T> h = head;
            Node<T> first = h.next;
            h.next = h; // help GC
            head = first;
            T x = first.item;
            first.item = null;
            return x;
        }

        @Override
        public T poll(long timeout, TimeUnit unit) throws InterruptedException {
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
        public boolean addAll(Collection<? extends T> c) {
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
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<T> iterator() {
            return null;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            return null;
        }

        @Override
        public int drainTo(Collection<? super T> c) {
            return 0;
        }

        @Override
        public int drainTo(Collection<? super T> c, int maxElements) {
            return 0;
        }

        static class Node<E> {
            E item;

            /**
             * One of:
             * - the real successor Node
             * - this Node, meaning the successor is head.next
             * - null, meaning there is no successor (this is the last node)
             */
            Node<E> next;

            Node(E x) { item = x; }
        }
    }


}
