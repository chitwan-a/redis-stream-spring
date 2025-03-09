class BankAccount {
    private int balance;

    public BankAccount(int balance) {
        this.balance = balance;
    }

    public void withdraw(int amount) {
        if (balance >= amount) {
            System.out.println(Thread.currentThread().getName() + " is about to withdraw $" + amount);
            try {
                Thread.sleep(100); // Simulate some processing time
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            balance -= amount;
            System.out.println(Thread.currentThread().getName() + " completed the withdrawal. Remaining balance: $" + balance);
        } else {
            System.out.println(Thread.currentThread().getName() + " tried to withdraw $" + amount + " but insufficient balance!");
        }
    }

    public int getBalance() {
        return balance;
    }
}

public class ConcurrencyIssueExample {
    public static void main(String[] args) {
        BankAccount account = new BankAccount(100); // Initial balance: $100

        Runnable withdrawTask = () -> {
            for (int i = 0; i < 2; i++) {
                account.withdraw(70); // Each thread tries to withdraw $70
            }
        };

        Thread t1 = new Thread(withdrawTask, "Thread-1");
        Thread t2 = new Thread(withdrawTask, "Thread-2");

        t1.start();
        t2.start();
    }
}
