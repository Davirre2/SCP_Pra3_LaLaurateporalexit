package eps.scp;

import eps.scp.InvertedIndex;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class FileSearchThread extends Thread {
    private String dirpath;
    private InvertedIndex invertedIndex;
    private CountDownLatch latch;

    public FileSearchThread(String dirpath, InvertedIndex invertedIndex, CountDownLatch latch) {
        this.dirpath = dirpath;
        this.invertedIndex = invertedIndex;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            invertedIndex.searchDirectoryFiles(dirpath);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        latch.countDown();
    }
}
