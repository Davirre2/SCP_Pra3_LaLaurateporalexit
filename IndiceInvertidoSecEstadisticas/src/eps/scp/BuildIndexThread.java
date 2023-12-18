package eps.scp;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class BuildIndexThread extends Thread {
    private int fileId;
    private File file;
    private InvertedIndex invertedIndex;
    private Map<Integer, String> Files = new HashMap<Integer, String>();
    private CyclicBarrier barrier;

    public BuildIndexThread(int fileId, File file, InvertedIndex invertedIndex, CyclicBarrier barrier) {
        this.fileId = fileId;
        this.file = file;
        this.invertedIndex = invertedIndex;
        this.barrier = barrier;
    }


    @Override
    public void run() {
        //Crida funció
        invertedIndex.addFileWords2Index(fileId, file);
        try {
            barrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
}
