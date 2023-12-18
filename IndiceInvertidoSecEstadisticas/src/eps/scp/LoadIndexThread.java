package eps.scp;

import java.io.*;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoadIndexThread extends Thread {
    private File file;
    private InvertedIndex invertedIndex;
    private Lock lock;
    private Condition condition;

    LoadIndexThread(File file, InvertedIndex invertedIndex, Lock lock, Condition condition){
        this.file = file;
        this.invertedIndex = invertedIndex;
        this.lock = lock;
        this.condition = condition;
    }

    @Override
    public void run(){
        try {
            FileReader input = new FileReader(file);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try {
                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    HashSet<Location> locationsList = new HashSet<Location>();
                    // Descomponemos la línea leída en su clave (word) y las ubicaciones
                    String[] fields = keyLine.split("\t");
                    String word = fields[0];
                    String[] locations = fields[1].split(", ");
                    // Recorremos los offsets para esta clave y los añadimos al HashMap
                    for (int i = 0; i < locations.length; i++)
                    {
                        String[] location = locations[i].substring(1, locations[i].length()-1).split(",");
                        int fileId = Integer.parseInt(location[0]);
                        int line = Integer.parseInt(location[1]);
                        locationsList.add(new Location(fileId,line));
                    }

                    lock.lock();
                    try{
                        invertedIndex.getHash().put(word, locationsList);
                        condition.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading Index file");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Index file");
            e.printStackTrace();
        }
    }
}
