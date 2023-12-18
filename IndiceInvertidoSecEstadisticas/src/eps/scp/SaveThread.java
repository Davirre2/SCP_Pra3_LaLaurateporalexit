package eps.scp;

import eps.scp.Location;

import java.io.*;
import java.nio.charset.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SaveThread extends Thread {
    private final Map<String, HashSet<Location>> Hash;
    private final String outputDirectory;
    private final String DIndexFilePrefix;
    private final int keysPerThread;
    private final Iterator<String> keyIterator;
    private final Phaser phaser;
    private final Lock lock;

    public SaveThread(Map<String, HashSet<Location>> hash, String outputDirectory, String dIndexFilePrefix, int keysPerThread, Iterator<String> keyIterator, Phaser phaser, Lock lock) {
        this.Hash = hash;
        this.outputDirectory = outputDirectory;
        this.DIndexFilePrefix = dIndexFilePrefix;
        this.keysPerThread = keysPerThread;
        this.keyIterator = keyIterator;
        this.phaser = phaser;
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            lock.lock();
            // Crear l'arxiu d'índex corresponent al fil
            File KeyFile = new File(outputDirectory + "/" + DIndexFilePrefix + getName());
            FileWriter fw = new FileWriter(KeyFile);
            BufferedWriter bw = new BufferedWriter(fw);

            int keysWritten = 0;
            while (keyIterator.hasNext() && keysWritten < keysPerThread) {
                String key = keyIterator.next();
                saveIndexKey(key, bw); // Guardar la clau a l'arxiu de l'índex
                keysWritten++;
            }

            bw.close();
            phaser.arrive();
        } catch (IOException e) {
            System.err.println("Error creating Index file");
            e.printStackTrace();
            System.exit(-1);
        }
        lock.unlock();
    }

    // Guardar la clau i les seves ubicacions a un arxiu
    private void saveIndexKey(String key, BufferedWriter bw) {
        try {
            HashSet<Location> locations = Hash.get(key);
            String joined = String.join(";", locations.toString());
            bw.write(key + "\t" + joined.substring(1, joined.length() - 1) + "\n");
        } catch (IOException e) {
            System.err.println("Error writing Index file");
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

