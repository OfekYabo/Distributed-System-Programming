package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;
import com.distributed.systems.shared.model.WorkerTaskMessage;
import com.distributed.systems.shared.service.S3Service;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.nndep.DependencyParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Processes text analysis tasks.
 * optimised for memory usage and simplicity.
 * Streams input directly from URL to NLP parser without saving input file to
 * disk.
 * Uses shared models to reduce memory footprint.
 */
public class TaskProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TaskProcessor.class);

    // Config Keys
    private static final String TEMP_DIR_KEY = "TEMP_DIR";
    private static final String S3_BUCKET_KEY = "S3_BUCKET_NAME";
    private static final String S3_RESULTS_PREFIX_KEY = "S3_WORKER_RESULTS_PREFIX";
    private static final String MAX_RAM_BUFFER_KEY = "WORKER_MAX_RAM_BUFFER_MB";
    private static final String MAX_SENTENCE_LENGTH_KEY = "WORKER_MAX_SENTENCE_LENGTH";

    private final S3Service s3Service;

    // Config Values
    private final String tempDir;
    private final String s3BucketName;
    private final String s3ResultsPrefix;
    private final int maxRamBufferSize;
    private final int maxSentenceLength;

    // Singleton models to save memory (loaded lazily)
    private static volatile MaxentTagger sharedTagger;
    private static volatile LexicalizedParser constituencyParser;
    private static volatile DependencyParser dependencyParser;

    // Model paths
    private static final String POS_MODEL_PATH = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger";
    private static final String PARSER_MODEL_PATH = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";

    public TaskProcessor(AppConfig config, S3Service s3Service) {
        this.s3Service = s3Service;

        // Load Configuration
        this.tempDir = config.getString(TEMP_DIR_KEY);
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.s3ResultsPrefix = config.getOptional(S3_RESULTS_PREFIX_KEY, "workers-results");
        // Default to 50MB if not specified
        this.maxRamBufferSize = config.getIntOptional(MAX_RAM_BUFFER_KEY, 50) * 1024 * 1024;
        this.maxSentenceLength = config.getIntOptional(MAX_SENTENCE_LENGTH_KEY, 80);

        ensureTempDirectory();
    }

    private void ensureTempDirectory() {
        try {
            Files.createDirectories(Paths.get(tempDir));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }
    }

    /**
     * Processes a task by streaming data from the URL, parsing it, and uploading
     * the result.
     * Tasks can run as long as needed - heartbeat mechanism keeps the message invisible.
     */
    public String processTask(WorkerTaskMessage.TaskData taskData, long ignoredDeadline) throws Exception {
        String url = taskData.getUrl();
        String method = taskData.getParsingMethod();
        logger.info("Processing task: URL={}, Method={}", url, method);

        // Prepare temporary files (lazily created if needed)
        String outputFilename = "result_" + UUID.randomUUID() + ".txt";
        Path outputPath = Paths.get(tempDir, outputFilename);

        // We might or might not use a temp input file
        Path tempInputPath = null;
        byte[] memoryBuffer = null;

        try {
            // 1. Download Content (Hybrid: RAM first, Spill to Disk if large)
            logger.info("Downloading content...");
            URLConnection connection = URI.create(url).toURL().openConnection();

            try (InputStream in = connection.getInputStream()) {
                ByteArrayOutputStream ramBuffer = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192]; // 8KB chunks
                int bytesRead;
                boolean spilledToDisk = false;

                // Read from network
                while ((bytesRead = in.read(buffer)) != -1) {
                    ramBuffer.write(buffer, 0, bytesRead);

                    // Check if we exceeded RAM limit
                    if (!spilledToDisk && ramBuffer.size() > maxRamBufferSize) {
                        logger.info(">>> [DISK SPILL] <<< File larger than {} MB, spilling to disk...",
                                maxRamBufferSize / (1024 * 1024));
                        spilledToDisk = true;

                        // Create temp file and write what we have so far
                        tempInputPath = Paths.get(tempDir, "input_" + UUID.randomUUID() + ".txt");
                        try (OutputStream fileOut = Files.newOutputStream(tempInputPath)) {
                            ramBuffer.writeTo(fileOut);
                            // Clear RAM buffer to free memory
                            ramBuffer = null;

                            // Continue writing remainder of stream directly to file
                            while ((bytesRead = in.read(buffer)) != -1) {
                                fileOut.write(buffer, 0, bytesRead);
                            }
                        }
                        break; // We are done reading the stream into the file
                    }
                }

                if (!spilledToDisk) {
                    memoryBuffer = ramBuffer.toByteArray();
                    logger.warn(">>> [DOWNLOAD COMPLETE] <<< Kept in RAM ({} bytes).", memoryBuffer.length);
                } else {
                    logger.warn(">>> [DOWNLOAD COMPLETE] <<< Spilled to disk ({}).", tempInputPath);
                }
            }

            // 2. Process (from RAM or Disk)
            InputStream processingStream;
            long totalSize;
            if (memoryBuffer != null) {
                processingStream = new ByteArrayInputStream(memoryBuffer);
                totalSize = memoryBuffer.length;
            } else {
                processingStream = Files.newInputStream(tempInputPath);
                totalSize = Files.size(tempInputPath);
            }

            try (InputStream inStream = processingStream;
                    BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

                processStream(inStream, writer, method, totalSize);
            }

            // 3. Upload result to S3
            String s3Key = generateS3Key(url, method);
            s3Service.uploadFile(outputPath, s3Key);

            String s3Url = "s3://" + s3BucketName + "/" + s3Key;
            logger.warn(">>> [TASK COMPLETE] <<< S3 URL: {}", s3Url);
            return s3Url;

        } finally {
            // Cleanup temp files
            try {
                if (outputPath != null)
                    Files.deleteIfExists(outputPath);
                if (tempInputPath != null)
                    Files.deleteIfExists(tempInputPath);
            } catch (IOException e) {
                logger.warn("Failed to delete temp files", e);
            }
        }
    }

    /**
     * Core processing loop. Reads sentences from stream and applies appropriate
     * parser. No timeout - tasks can run as long as needed.
     */
    private void processStream(InputStream input, BufferedWriter writer, String method, long totalBytes)
            throws Exception {
        // Prepare the specific processor logic based on method
        BiConsumer<List<HasWord>, BufferedWriter> sentenceProcessor = getSentenceProcessor(method);

        // Simple wrapper to track bytes read for progress calculation
        final long[] bytesRead = { 0 };
        InputStream countingStream = new FilterInputStream(input) {
            @Override
            public int read() throws IOException {
                int b = super.read();
                if (b != -1)
                    bytesRead[0]++;
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int n = super.read(b, off, len);
                if (n > 0)
                    bytesRead[0] += n;
                return n;
            }
        };

        // DocumentPreprocessor iterates over sentences directly from the stream
        DocumentPreprocessor dp = new DocumentPreprocessor(new InputStreamReader(countingStream));

        int sentenceCount = 0;
        int lastLoggedPercent = 0;

        for (List<HasWord> sentence : dp) {
            if (sentence == null || sentence.isEmpty())
                continue;

            try {
                sentenceProcessor.accept(sentence, writer);
            } catch (Exception e) {
                logger.warn("Error processing sentence: {}", e.getMessage());
                writer.write("[ERROR: " + e.getMessage() + "]\n");
            }

            sentenceCount++;

            // Calculate progress based on bytes read
            if (totalBytes > 0) {
                int currentPercent = (int) ((bytesRead[0] * 100) / totalBytes);
                // Log every 5% progress
                if (currentPercent >= lastLoggedPercent + 5) {
                    lastLoggedPercent = currentPercent;
                    logger.info(">>> [PROGRESS] <<< {}% (Processed {} sentences)", currentPercent, sentenceCount);
                }
            } else if (sentenceCount % 100 == 0) {
                // Fallback if total size unknown
                logger.info("Processed {} sentences...", sentenceCount);
            }
        }
        logger.info(">>> [FINISHED] <<< Processed {} total sentences.", sentenceCount);
    }

    /**
     * Returns the appropriate processing logic for the method.
     */
    private BiConsumer<List<HasWord>, BufferedWriter> getSentenceProcessor(String method) throws Exception {
        switch (method) {
            case "POS":
                ensureTaggerLoaded();
                return this::processPOS;
            case "CONSTITUENCY":
                ensureConstituencyLoaded();
                return this::processConstituency;
            case "DEPENDENCY":
                ensureTaggerLoaded(); // Dependency parser needs the tagger too
                ensureDependencyLoaded();
                return this::processDependency;
            default:
                throw new IllegalArgumentException("Unknown parsing method: " + method);
        }
    }

    // --- Specific Processors ---

    private void processPOS(List<HasWord> sentence, BufferedWriter writer) {
        try {
            List<TaggedWord> tagged = sharedTagger.tagSentence(sentence);
            for (TaggedWord word : tagged) {
                writer.write(word.word() + "/" + word.tag() + " ");
            }
            writer.write("\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processConstituency(List<HasWord> sentence, BufferedWriter writer) {
        try {
            if (sentence.size() > maxSentenceLength) {
                logger.warn(">>> [SKIPPED LONG SENTENCE] <<< Size: {} words", sentence.size());
                writer.write("[SKIPPED LONG SENTENCE: " + sentence.size() + " words]\n");
                return;
            }
            Tree tree = constituencyParser.apply(sentence);
            writer.write(tree.toString());
            writer.write("\n\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processDependency(List<HasWord> sentence, BufferedWriter writer) {
        try {
            List<TaggedWord> tagged = sharedTagger.tagSentence(sentence);
            GrammaticalStructure gs = dependencyParser.predict(tagged);
            writer.write(gs.toString());
            writer.write("\n\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- Lazy Loaders with DCL ---

    private void ensureTaggerLoaded() {
        if (sharedTagger == null) {
            synchronized (TaskProcessor.class) {
                if (sharedTagger == null) {
                    logger.info("Loading Shared POS Tagger...");
                    sharedTagger = new MaxentTagger(POS_MODEL_PATH);
                }
            }
        }
    }

    private void ensureConstituencyLoaded() {
        if (constituencyParser == null) {
            synchronized (TaskProcessor.class) {
                if (constituencyParser == null) {
                    logger.info("Loading Constituency Parser...");
                    constituencyParser = LexicalizedParser.loadModel(PARSER_MODEL_PATH);
                }
            }
        }
    }

    private void ensureDependencyLoaded() {
        if (dependencyParser == null) {
            synchronized (TaskProcessor.class) {
                if (dependencyParser == null) {
                    logMemoryStatus("Before loading Dependency Parser");
                    logger.info("Loading Dependency Parser (model: {})...", DependencyParser.DEFAULT_MODEL);

                    long startTime = System.currentTimeMillis();
                    try {
                        dependencyParser = DependencyParser.loadFromModelFile(DependencyParser.DEFAULT_MODEL);
                        long elapsed = System.currentTimeMillis() - startTime;
                        logger.info("Dependency Parser loaded successfully in {} ms", elapsed);
                        logMemoryStatus("After loading Dependency Parser");
                    } catch (OutOfMemoryError e) {
                        logger.error(
                                "OUT OF MEMORY loading Dependency Parser! Increase JVM heap size (-Xmx). Current max heap: {} MB",
                                Runtime.getRuntime().maxMemory() / (1024 * 1024));
                        throw e;
                    }
                }
            }
        }
    }

    private void logMemoryStatus(String phase) {
        Runtime rt = Runtime.getRuntime();
        long usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long maxMB = rt.maxMemory() / (1024 * 1024);
        long freeMB = rt.freeMemory() / (1024 * 1024);
        logger.info("[Memory {}] Used: {} MB, Free: {} MB, Max: {} MB", phase, usedMB, freeMB, maxMB);
    }

    // --- Output Naming Utilities ---

    private String generateS3Key(String originalUrl, String method) {
        String timestamp = String.valueOf(Instant.now().toEpochMilli());
        String filename = extractFilename(originalUrl);
        return String.format("%s/%s/%s-%s.txt",
                s3ResultsPrefix,
                timestamp, method, filename);
    }

    private String extractFilename(String url) {
        try {
            String f = url.substring(url.lastIndexOf('/') + 1);
            if (f.contains("."))
                f = f.substring(0, f.lastIndexOf('.'));
            return f.replaceAll("[^a-zA-Z0-9-_]", "_");
        } catch (Exception e) {
            return "output";
        }
    }

    public static boolean isValidParsingMethod(String method) {
        return "POS".equals(method) || "CONSTITUENCY".equals(method) || "DEPENDENCY".equals(method);
    }

    /**
     * InputStream wrapper that tracks bytes read and logs progress at 10%
     * intervals.
     */
    @SuppressWarnings("unused")
    private static class ProgressTrackingInputStream extends FilterInputStream {
        private final long totalBytes;
        private final String url;
        private long bytesRead = 0;
        private int lastLoggedPercent = 0;
        private static final int LOG_INTERVAL = 10; // Log every 10%

        public ProgressTrackingInputStream(InputStream in, long totalBytes, String url) {
            super(in);
            this.totalBytes = totalBytes;
            this.url = extractShortName(url);
            if (totalBytes > 0) {
                logger.info("Starting processing of {} ({} bytes)", this.url, totalBytes);
            } else {
                logger.info("Starting processing of {} (unknown size)", this.url);
            }
        }

        private String extractShortName(String url) {
            try {
                String name = url.substring(url.lastIndexOf('/') + 1);
                if (name.length() > 30) {
                    name = name.substring(0, 27) + "...";
                }
                return name;
            } catch (Exception e) {
                return "file";
            }
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) {
                bytesRead++;
                checkProgress();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                bytesRead += n;
                checkProgress();
            }
            return n;
        }

        private void checkProgress() {
            if (totalBytes <= 0)
                return; // Can't calculate progress without total size

            int currentPercent = (int) ((bytesRead * 100) / totalBytes);
            int currentInterval = (currentPercent / LOG_INTERVAL) * LOG_INTERVAL;

            if (currentInterval > lastLoggedPercent && currentInterval < 100) {
                lastLoggedPercent = currentInterval;
                logger.info("Processing {}: {}% complete ({}/{} bytes)",
                        url, currentInterval, bytesRead, totalBytes);
            }
        }

        public void logFinalProgress() {
            if (totalBytes > 0) {
                logger.info("Processing {}: 100% complete ({} bytes processed)", url, bytesRead);
            } else {
                logger.info("Processing {}: complete ({} bytes processed)", url, bytesRead);
            }
        }
    }
}
