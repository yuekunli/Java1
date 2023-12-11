package MinioTest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MinioMultiThreadTest {
    private final String[] keyIds = {"user1", "user2", "user3", "user4"};
    private final String[] secretKeys = {"KnockOnDoor1", "KnockOnDoor2", "KnockOnDoor3", "KnockOnDoor4"};

    public void run()
    {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++)
        {
            executor.execute(new MinioTestWorker(keyIds[i], secretKeys[i]));
        }
        executor.shutdown();
    }
}
