class Counter {
    private int count = 0;  // Shared variable

    public void increment() {
        int temp = count;  // Thread reads the value
        try {
            Thread.sleep(50); // Simulate some processing delay
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        count = temp + 1;  // Thread writes back the incremented value
    }

    public int getCount() {
        return count;
    }
}

public class ConcurrencyRuntimeIssue {
    public static void main(String[] args) throws InterruptedException {
        Counter counter = new Counter();

        Runnable task = () -> {
            for (int i = 0; i < 5; i++) {
                counter.increment();
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.println("Final Count: " + counter.getCount()); // Expected: 10, but may be lower due to lost updates
    }
}
