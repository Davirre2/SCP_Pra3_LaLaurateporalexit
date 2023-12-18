package eps.scp;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.units.qual.C;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InvertedIndex
{
    // Constantes
    public final String ANSI_RED = "\u001B[31m";
    public final String ANSI_GREEN = "\u001B[32m";
    public final String ANSI_BLUE = "\u001B[34m";
    public final String ANSI_GREEN_YELLOW_UNDER = "\u001B[32;40;4m";
    public final String ANSI_RESET = "\u001B[0m";
    private final int DIndexMaxNumberOfFiles = 200;   // Número máximo de ficheros para salvar el índice invertido.
    private final int DIndexMinNumberOfFiles = 2;     // Número mínimo de ficheros para salvar el índice invertido.
    private final int DKeysByFileIndex = 1000;
    private final String DIndexFilePrefix = "IndexFile";   // Prefijo de los ficheros de Índice Invertido.
    private final String DFileLinesName = "FilesLinesContent";  // Nombre fichero donde guardar las lineas de los ficheros indexados
    private final String DFilesIdsName = "FilesIds";  // Nombre fichero donde guardar las identificadores de los ficheros indexados
    private final String DDefaultIndexDir = "./Index/";   // Directorio por defecto donde se guarda el indice invertido.

    private final float DMatchingPercentage = 0.80f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)
    private final float DNearlyMatchingPercentage = 0.60f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)

    // Members
    private String InputDirPath = null;       // Contiene la ruta del directorio que contiene los ficheros a Indexar.
    private String IndexDirPath = null;       // Contiene la ruta del directorio donde guardar el indice.
    private RandomAccessFile randomInputFile;  // Fichero random para acceder al texto original con mayor porcentaje de matching.

        // Lista ne donde se guardas los ficheros a procesar
    private List<File> FilesList = new ArrayList<>();

    // Hash Map convertir de ids ficheros a su ruta
    private Map<Integer,String> Files = new HashMap<Integer,String>();

    // Hash Map para acceder a las líneas de todos los ficheros del indice.
    private Map<Location, String> IndexFilesLines = new TreeMap<Location, String>();

    // Hash Map que implementa el Índice Invertido: key=word, value=Locations(Listof(file,line)).
    private Map<String, HashSet <Location>> Hash =  new TreeMap<String, HashSet <Location>>();

    // Estadisticas para verificar la correcta contrucción del indice invertido.
    private Statistics GlobalStatistics = new Statistics("=");
    private long TotalLocations = 0;
    private long TotalWords = 0;
    private long TotalLines = 0;
    private int TotalProcessedFiles = 0;
    private final Lock lock = new ReentrantLock();
    private final Semaphore semaphore = new Semaphore(1);
    private Condition condition = lock.newCondition();


    // Getters
    public Map<Integer, String> getFiles() { return Files; }
    public Map<Location, String> getIndexFilesLines() { return IndexFilesLines; }
    public Map<String, HashSet<Location>> getHash() { return Hash; }
    public void setIndexDirPath(String indexDirPath) {
        IndexDirPath = indexDirPath;
    }
    public long getTotalWords(Map hash){ return(hash.size()); }
    public long getTotalLocations() { return TotalLocations; }
    public long getTotalWords() { return TotalWords; }
    public long getTotalLines() { return TotalLines; }
    public int getTotalProcessedFiles() { return TotalProcessedFiles; }


    // Constructores
    public InvertedIndex() {
    }
    public InvertedIndex(String InputPath) {
        this.InputDirPath = InputPath;
        this.IndexDirPath = DDefaultIndexDir;
    }

    public InvertedIndex(String inputDir, String indexDir) {
        this.InputDirPath = inputDir;
        this.IndexDirPath = indexDir;
    }

    // Método para la construcción del indice invertido.
    //  1. Busca los ficheros de texto recursivamente en el directorio de entrada.
    //  2. Construye el indice procesando las palabras del fichero.
    public void buidIndex() throws InterruptedException, BrokenBarrierException {
        Instant start = Instant.now();

        TotalProcessedFiles = 0;
        TotalLocations = 0;
        TotalLines=0;
        TotalWords=0;
        semaphore.release();
        searchDirectoryFiles(InputDirPath);
        buidIndexFiles();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Build Index with %d files] Total execution time: %.3f secs.\n", FilesList.size(), timeElapsed/1000.0);

        // Comprobar que el resultado sea correcto.
        try {
            assertEquals(getTotalWords(Hash), getTotalWords());
            assertEquals(getTotalLocations(Hash), getTotalLocations());
            assertEquals(getTotalFiles(Files), getTotalProcessedFiles());
            assertEquals(getTotalLines(IndexFilesLines), getTotalLines());
        }catch (AssertionError e){
            System.out.println(ANSI_RED+ e.getMessage() + " "+ ANSI_RESET);
        }
    }

    // Calcula el número de ubicaciones diferentes de las palabras en los ficheros.
    // Si una palabra aparece varias veces en un linea de texto, solo se cuenta una vez.
    public long getTotalLocations(Map hash)
    {
        long locations=0;
        Set<String> keySet = hash.keySet();

        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            locations += Hash.get(word).size();
            //HashSet<Location> locs = Hash.get(word);
            //String joined = String.join(";",locs.toString());
            //System.out.printf(ANSI_BLUE+"[%d-%d] %s --> %s\n"+ANSI_RESET,locations-locs.size(),locations, word, joined);
        }
        return(locations);
    }
    public long getTotalFiles(Map files){
        return(files.size());
    }
    public long getTotalLines(Map filesLines){
        return(filesLines.size());
    }

    // Procesamiento recursivo del directorio para buscar los ficheros de texto.
    // Cada fichero encontrado se guarda en la lista fileList
    public void searchDirectoryFiles(String dirpath) throws InterruptedException {
        File file=new File(dirpath);
        File[] content = file.listFiles();
        if (content != null) {
            CountDownLatch latch = new CountDownLatch(content.length);
            for (int i = 0; i < content.length; i++) {
                if (content[i].isDirectory()) {
                    // Si és un directori es processa a un nou fil, que cridarà la funció un altre cop
                    int finalI = i;
                    Thread fileSearchThread = new FileSearchThread(content[finalI].getAbsolutePath(), this, latch);
                    fileSearchThread.start();
                } else {
                    latch.countDown();
                    // Si és un fitxer de text, s'afegeix a la llista de fitxers
                    if (checkFile(content[i].getName())) {
                        lock.lock();
                        FilesList.add(content[i]);
                        lock.unlock();
                    }
                }
            }
            //Fem servir el CountDownLatch per tals d'assegurar-nos que tots han acabat abans de donar per acabada la cerca de directoris.
            latch.await();
        } else {
            System.err.printf("Directori %s no existeix.\n", file.getAbsolutePath());
        }
    }

    // Método para Contruir el Indice Invertido a partir de todos los ficheros de texto encontrados
    // en el árbol de directorios.
    // En el Indice invertido se almacena los ids lógicos de los ficheros procesados. La correspondencia
    // idFichero->RutaNombreFichero se guarda en el hash Files
    public void buidIndexFiles() throws BrokenBarrierException, InterruptedException {
        int fileId=0;
        CyclicBarrier barrier = new CyclicBarrier(FilesList.size() + 1);
        for (File file : FilesList)
        {
            fileId++;                                   // Incrementar Identificador fichero
            Files.put(fileId, file.getAbsolutePath());  // Añadir entra en el hash de traducción de Id a ruta+nombre fichero

            Thread buildThread = new BuildIndexThread(fileId, file, this, barrier);
            buildThread.start();
        }

        barrier.await();
        synchronized (this){
            String maxWord = Collections.max(Hash.entrySet(), (entry1, entry2) -> entry1.getValue().size() - entry2.getValue().size()).getKey();
            GlobalStatistics.setMostPopularWord(maxWord);
            GlobalStatistics.setMostPopularWordLocations(Hash.get(maxWord).size());
            GlobalStatistics.print("Global");
        }
    }

    public synchronized void addFileWords2Index(int fileId, File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int lineNumber = 0;

                Statistics fileStatistics = new Statistics("_");
                System.out.printf("Processing %3dth file %s (Path: %s)\n", fileId, file.getName(), file.getAbsolutePath());
                TotalProcessedFiles++;
                fileStatistics.incProcessingFiles();
                while ((line = br.readLine()) != null) {
                    lineNumber++;
                    TotalLines++;
                    fileStatistics.incProcessedLines();
                    if (Indexing.Verbose) System.out.printf("Procesando linea %d fichero %d: ", lineNumber, fileId);

                    Location newLocation = new Location(fileId, lineNumber);
                    addIndexFilesLine(newLocation, line);

                    line = Normalizer.normalize(line, Normalizer.Form.NFD);
                    line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
                    String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");

                    String[] words = filter_line.split("\\W+");

                    for (String word : words) {
                        if (Indexing.Verbose) System.out.printf("%s ", word);
                        word = word.toLowerCase();
                        HashSet<Location> locations = Hash.get(word);

                        if (locations == null) {
                            locations = new HashSet<>();
                            if (!Hash.containsKey(word)) fileStatistics.incKeysFound();
                            Hash.put(word, locations);
                            TotalWords++;
                            fileStatistics.incProcessedWords();
                        }

                        int oldLocSize = locations.size();
                        locations.add(newLocation);

                        if (locations.size() > oldLocSize) {
                            TotalLocations++;
                            fileStatistics.incProcessedLocations();
                        }
                    }

                    if (Indexing.Verbose) System.out.println();
                }
                fileStatistics.incProcessedFiles();
                fileStatistics.decProcessingFiles();
                setMostPopularWord(fileStatistics);
                fileStatistics.print(file.getName());

                GlobalStatistics.addStatistics(fileStatistics);
        } catch (FileNotFoundException e) {
            System.err.printf("Fichero %s no encontrado.\n", file.getAbsolutePath());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.printf("Error lectura fichero %s.\n", file.getAbsolutePath());
            e.printStackTrace();
        }
    }

    public synchronized void setMostPopularWord(Statistics stats) {
        String maxWord = Collections.max(Hash.entrySet(), (entry1, entry2) -> entry1.getValue().size() - entry2.getValue().size()).getKey();
        stats.setMostPopularWord(maxWord);
        stats.setMostPopularWordLocations(Hash.get(maxWord).size());
    }


    // Verificar si la extensión del fichero coincide con la extensiones buscadas (txt)
    private boolean checkFile (String name)
    {
        if (name.endsWith("txt")) {
            return true;
        }
        return false;
    }

    // Método para imprimir por pantalla el índice invertido.
    public void printIndex()
    {
        Set<String> keySet = Hash.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            System.out.print(word + "\t");
            HashSet<Location> locations = Hash.get(word);
            for(Location loc: locations){
                System.out.printf("(%d,%d) ", loc.getFileId(), loc.getLine());
            }
            System.out.println();
        }
    }


    public void saveIndex() throws InterruptedException {
        saveIndex(IndexDirPath);
    }

    public void saveIndex(String indexDirectory) throws InterruptedException {
        Instant start = Instant.now();

        resetDirectory(indexDirectory);

        // Crear un fil per cada arxiu a la llista
        saveInvertedIndex(indexDirectory);

        CountDownLatch latch = new CountDownLatch(2);

        // Crear un fil per guardar els fileIDs
        Thread filesIdsThread = new Thread(() -> saveFilesIds(indexDirectory, latch));
        filesIdsThread.start();

        // Crear un fil per guardar les fileLines
        Thread filesLinesThread = new Thread(() -> saveFilesLines(indexDirectory, latch));
        filesLinesThread.start();

        latch.await();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Save Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed/1000.0);
    }

    public void resetDirectory(String outputDirectory)
    {
        File path = new File(outputDirectory);
        if (!path.exists())
            path.mkdir();
        else if (path.isDirectory()) {
            try {
                FileUtils.cleanDirectory(path);
            } catch (IOException e) {
                System.err.printf("Error borrando contenido directorio indice %s.\n",path.getAbsolutePath());
                e.printStackTrace();
            }
        }
    }
    public void saveInvertedIndex(String outputDirectory) {
        try {
            Charset utf8 = StandardCharsets.UTF_8;
            Set<String> keySet = Hash.keySet();

            int numberOfFiles = keySet.size() / DKeysByFileIndex;
            if (numberOfFiles > DIndexMaxNumberOfFiles) {
                numberOfFiles = DIndexMaxNumberOfFiles;
            }
            if (numberOfFiles < DIndexMinNumberOfFiles) {
                numberOfFiles = DIndexMinNumberOfFiles;
            }

            // Dividir el treball entre els fils
            int keysPerThread = Math.min(DKeysByFileIndex, keySet.size()); // Limita a keysPerDocument o menys
            Iterator<String> keyIterator = keySet.iterator();
            Phaser phaser = new Phaser(numberOfFiles + 1);

            for (int f = 1; f <= numberOfFiles; f++) {
                Thread saveThread = new SaveThread(Hash, outputDirectory, DIndexFilePrefix, keysPerThread, keyIterator, phaser, lock);
                saveThread.start();
            }
            // Esperar a que tots els fils acabin
            phaser.arriveAndAwaitAdvance();
        } catch (Exception e) {
            System.err.println("Error while saving inverted index: " + e.getMessage());
        }
    }

    public void saveFilesIds(String outputDirectory, CountDownLatch latch)
    {
        try {
            semaphore.acquire();
            FileWriter fw = new FileWriter(outputDirectory + "/" + DFilesIdsName);
            BufferedWriter bw = new BufferedWriter(fw);
            Set<Entry<Integer,String>> keySet = Files.entrySet();
            Iterator keyIterator = keySet.iterator();

            while (keyIterator.hasNext() )
            {
                Entry<Integer,String> entry = (Entry<Integer,String>) keyIterator.next();
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
            bw.close(); // Cerramos el fichero.

        } catch (IOException e) {
            System.err.println("Error creating FilesIds file: " + outputDirectory + DFilesIdsName + "\n");
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            latch.countDown();
            semaphore.release();
        }
    }

    public void saveFilesLines(String outputDirectory, CountDownLatch latch)
    {
        try {
            semaphore.acquire();
            File KeyFile = new File(outputDirectory + "/" + DFileLinesName);
            FileWriter fw = new FileWriter(KeyFile);
            BufferedWriter bw = new BufferedWriter(fw);
            Set<Entry<Location, String>> keySet = IndexFilesLines.entrySet();
            Iterator keyIterator = keySet.iterator();

            while (keyIterator.hasNext() )
            {
                Entry<Location, String> entry = (Entry<Location, String>) keyIterator.next();
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
            bw.close(); // Cerramos el fichero.
        } catch (IOException e) {
            System.err.println("Error creating FilesLines contents file: " + outputDirectory + DFileLinesName + "\n");
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            latch.countDown();
            semaphore.release();
        }
    }

    public void loadIndex() throws InterruptedException {
        loadIndex(IndexDirPath);
    }

    // Método para carga el indice invertido del directorio pasado como parámetro.
    // Se carga:
    //  + Indice Invertido (Hash)
    //  + Hash map de conversión de idFichero->RutaNombreFichero (Files)
    //  + Hash de acceso indexado a las lineas de los ficheros (IndexFilesLines)
    public void loadIndex(String indexDirectory) throws InterruptedException {
        Instant start = Instant.now();

        resetIndex();
        loadInvertedIndex(indexDirectory);
        CountDownLatch latch = new CountDownLatch(2);
        Thread filesIdsThread = new Thread(() -> loadFilesIds(indexDirectory, latch));
        filesIdsThread.start();

        // Crear un fil per guardar les fileLines
        Thread filesLinesThread = new Thread(() -> loadFilesLines(indexDirectory, latch));
        filesLinesThread.start();

        latch.await();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Load Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed/1000.0);
    }

    public void resetIndex()
    {
        Hash.clear();
        Files.clear();
        IndexFilesLines.clear();
    }

    // Método para cargar en memoria el índice invertido desde su copia en disco.
    public void loadInvertedIndex(String inputDirectory)
    {
        File folder = new File(inputDirectory);
        File[] listOfFiles = folder.listFiles((d, name) -> name.startsWith(DIndexFilePrefix));
        // Recorremos todos los ficheros del directorio de Indice y los procesamos.
        for (File file : listOfFiles) {
            if (file.isFile()) {
                LoadIndexThread thread = new LoadIndexThread(file, this, lock, condition);
                thread.start();
            }
        }
    }


    public void loadFilesIds(String inputDirectory, CountDownLatch latch)
    {
        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFilesIdsName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try {

                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (File Id) y la ruta del fichero.
                    String[] fields = keyLine.split("\t");
                    int fileId = Integer.parseInt(fields[0]);
                    fields[0]="";
                    String filePath = String.join("", fields);
                    Files.put(fileId, filePath);
                }
                bufRead.close();

            } catch (IOException e) {
                System.err.println("Error reading Files Ids");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Files Ids file");
            e.printStackTrace();
        }
        latch.countDown();
    }

    public void loadFilesLines(String inputDirectory, CountDownLatch latch)
    {
        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFileLinesName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try
            {
                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (Location) y la linea de texto correspondiente
                    String[] fields = keyLine.split("\t");
                    String[] location = fields[0].substring(1, fields[0].length()-1).split(",");
                    int fileId = Integer.parseInt(location[0]);
                    int line = Integer.parseInt(location[1]);
                    fields[0]="";
                    String textLine = String.join("", fields);
                    IndexFilesLines.put(new Location(fileId,line),textLine);
                }
                bufRead.close();

            } catch (IOException e) {
                System.err.println("Error reading Files Ids");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Files Ids file");
            e.printStackTrace();
        }
        latch.countDown();
    }


    // Implentar una consulta sobre el indice invertido:
    //  1. Descompone consulta en palabras.
    //  2. Optiene las localizaciones de cada palabra en el indice invertido.
    //  3. Agrupa palabras segun su localizacion en una hash de coincidencias.
    //  4. Recorremos la tabla de coincidencia y mostramos las coincidencias en función del porcentaje de matching.
    public void query(String queryString)
    {
        String queryResult=null;
        Map<Location, Integer> queryMatchings = new TreeMap<Location, Integer>();
        Instant start = Instant.now();

        System.out.println ("Searching for query: "+queryString);

        // Pre-procesamiento query
        queryString = Normalizer.normalize(queryString, Normalizer.Form.NFD);
        queryString = queryString.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        String filter_line = queryString.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]","");
        // Dividimos la línea en palabras.
        String[] words = filter_line.split("\\W+");
        int querySize = words.length;

        // Procesar cada palabra de la query
        for(String word: words)
        {
            word = word.toLowerCase();
            if (Indexing.Verbose) System.out.printf("Word %s matching: ",word);
            // Procesar las distintas localizaciones de esta palabra
            if (Hash.get(word)==null)
                continue;
            for(Location loc: Hash.get(word))
            {
                // Si no existe esta localización en la tabla de coincidencias, entonces la añadimos con valor inicial a 1.
                Integer value = queryMatchings.putIfAbsent(loc, 1);
                if (value != null) {
                    // Si existe, incrementamos el número de coincidencias para esta localización.
                    queryMatchings.put(loc, value+1);
                }
                if (Indexing.Verbose) System.out.printf("%s,",loc);
            }
            if (Indexing.Verbose) System.out.println(".");
        }

        if (queryMatchings.size()==0)
            System.out.printf(ANSI_RED+"Not matchings found.\n"+ANSI_RESET);

        // Recorremos la tabla de coincidencia y mostramos las líneas en donde aparezca más de un % de las palabras de la query.
        for(Map.Entry<Location, Integer> matching : queryMatchings.entrySet())
        {
            Location location = matching.getKey();
            if ((matching.getValue()/(float)querySize)==1.0)
                System.out.printf(ANSI_GREEN_YELLOW_UNDER+"%.2f%% Full Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DMatchingPercentage)
                System.out.printf(ANSI_GREEN+"%.2f%% Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DNearlyMatchingPercentage)
                System.out.printf(ANSI_RED+"%.2f%% Weak Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Query with %d words] Total execution time: %.3f secs.\n", querySize, timeElapsed/1000.0);
    }

    private String getIndexFilesLine(Location loc){
        return(IndexFilesLines.get(loc));
    }

    private void addIndexFilesLine(Location loc, String line){
        IndexFilesLines.put(loc, line);
    }

}
