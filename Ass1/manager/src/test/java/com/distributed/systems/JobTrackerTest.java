package com.distributed.systems;

import org.junit.Test;
import static org.junit.Assert.*;

public class JobTrackerTest {

    @Test
    public void testWorkerNeedsCalculation() {
        JobTracker jobTracker = new JobTracker();

        // Job 1: 10 tasks, n=1 -> Needs 10 workers
        jobTracker.registerJob("job1", 10, 1);
        assertEquals("Should need 10 workers initially", 10, jobTracker.getGlobalNeededWorkers());

        // Complete 1 task -> Needs 9 workers remaining
        // Logic: remaining=9, ceil(9/1)=9. Prev=10. Delta=-1.
        String completedId = jobTracker.recordTaskCompletion("job1");
        assertNull("Job should not be complete yet", completedId);
        assertEquals("Should need 9 workers after 1 completion", 9, jobTracker.getGlobalNeededWorkers());

        // Job 2: 10 tasks, n=2 -> Needs 5 workers
        // Total global: 9 + 5 = 14
        jobTracker.registerJob("job2", 10, 2);
        assertEquals("Should need 14 workers total", 14, jobTracker.getGlobalNeededWorkers());

        // Complete remaining 9 tasks for job1
        for (int i = 0; i < 8; i++) {
            jobTracker.recordTaskCompletion("job1");
        }
        // 1 remaining
        assertEquals("Should need 1 worker for job1", 1 + 5, jobTracker.getGlobalNeededWorkers());

        // Final task for job1
        completedId = jobTracker.recordTaskCompletion("job1");
        assertEquals("Job1 should be returned as complete", "job1", completedId);

        // Job 1 done. remaining=0. needed=0.
        // Total global: 0 + 5 = 5
        assertEquals("Should need 5 workers after job1 complete", 5, jobTracker.getGlobalNeededWorkers());

        // Remove job1 explicitly as Listener would
        jobTracker.removeJob("job1");
        assertEquals("Global workers should still be 5", 5, jobTracker.getGlobalNeededWorkers());

        // Complete job2
        for (int i = 0; i < 10; i++) {
            jobTracker.recordTaskCompletion("job2");
        }
        assertEquals("Global workers should be 0", 0, jobTracker.getGlobalNeededWorkers());
    }

    @Test
    public void testCeilingLogic() {
        JobTracker jobTracker = new JobTracker();
        // 10 tasks, n=3. 10/3 = 3.33 -> 4 workers needed.
        jobTracker.registerJob("job3", 10, 3);
        assertEquals("Should need 4 workers (ceil(10/3))", 4, jobTracker.getGlobalNeededWorkers());
    }
}
