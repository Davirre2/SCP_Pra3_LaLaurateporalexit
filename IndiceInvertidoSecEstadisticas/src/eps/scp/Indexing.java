package eps.scp;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BrokenBarrierException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Indexing
{
    public static final boolean Verbose = false;

    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        InvertedIndex hash;

        if (args.length <2 || args.length>2)
            System.err.println("Erro in Parameters. Usage: Indexing <SourceDirectory> [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex(args[0]);
        else
            hash = new InvertedIndex(args[0], args[1]);

        Instant start = Instant.now();

        hash.buidIndex();
        hash.saveIndex();
        Map<String, HashSet<Location>> old_hash = new TreeMap<String, HashSet <Location>>(hash.getHash());
        Map<Location, String> old_indexFilesLines = hash.getIndexFilesLines();
        Map<Integer, String> old_files = hash.getFiles();
        hash.loadIndex();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[All Stages] Total execution time: %.3f secs.\n", timeElapsed/1000.0);
    }
}
