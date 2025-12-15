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
    private static final String MAX_SENTENCE_LENGTH_KEY = "MAX_SENTENCE_LENGTH";
    private static final String S3_RESULTS_PREFIX_KEY = "S3_WORKER_RESULTS_PREFIX";

    private final S3Service s3Service;

    // Config Values
    private final String tempDir;
    private final String s3BucketName;
    private final int maxSentenceLength;
    private final String s3ResultsPrefix;

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
        this.maxSentenceLength = config.getIntOptional(MAX_SENTENCE_LENGTH_KEY, 100);
        this.s3ResultsPrefix = config.getOptional(S3_RESULTS_PREFIX_KEY, "workers-results");

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
     */
    public String processTask(WorkerTaskMessage.TaskData taskData) throws Exception {
        String url = taskData.getUrl();
        String method = taskData.getParsingMethod();
        logger.info("Processing task: URL={}, Method={}", url, method);

        // Prepare output file (we still write output to disk as a buffer before upload)
        String outputFilename = "result_" + UUID.randomUUID() + ".txt";
        Path outputPath = Paths.get(tempDir, outputFilename);

        try {
            // Open connection to get content length for progress tracking
            URLConnection connection = URI.create(url).toURL().openConnection();
            long contentLength = connection.getContentLengthLong();
            
            // Stream input directly from S3/URL and write to local output file
            try (InputStream rawStream = connection.getInputStream();
                    ProgressTrackingInputStream inputStream = new ProgressTrackingInputStream(rawStream, contentLength, url);
                    BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

                processStream(inputStream, writer, method);
                inputStream.logFinalProgress(); // Log 100% when done
            }

            // Upload to S3
            String s3Key = generateS3Key(url, method);
            s3Service.uploadFile(outputPath, s3Key);

            String s3Url = "s3://" + s3BucketName + "/" + s3Key;
            logger.info("Task processed successfully. S3 URL: {}", s3Url);
            return s3Url;

        } finally {
            // Cleanup output file
            try {
                Files.deleteIfExists(outputPath);
            } catch (IOException e) {
                logger.warn("Failed to delete temp file: {}", outputPath, e);
            }
        }
    }

    /**
     * Core processing loop. Reads sentences from stream and applies appropriate
     * parser.
     */
    private void processStream(InputStream input, BufferedWriter writer, String method) throws Exception {
        // Prepare the specific processor logic based on method
        BiConsumer<List<HasWord>, BufferedWriter> sentenceProcessor = getSentenceProcessor(method);

        // DocumentPreprocessor iterates over sentences directly from the stream
        DocumentPreprocessor dp = new DocumentPreprocessor(new InputStreamReader(input));

        int maxLen = maxSentenceLength;

        for (List<HasWord> sentence : dp) {
            if (sentence == null || sentence.isEmpty())
                continue;

            // Split long sentences into chunks to avoid memory issues
            for (int i = 0; i < sentence.size(); i += maxLen) {
                int end = Math.min(i + maxLen, sentence.size());
                List<HasWord> chunk = sentence.subList(i, end);

                try {
                    sentenceProcessor.accept(chunk, writer);
                } catch (Exception e) {
                    logger.warn("Error processing chunk: {}", e.getMessage());
                    writer.write("[ERROR: " + e.getMessage() + "]\n");
                }
            }
        }
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
                        logger.error("OUT OF MEMORY loading Dependency Parser! Increase JVM heap size (-Xmx). Current max heap: {} MB",
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
     * InputStream wrapper that tracks bytes read and logs progress at 10% intervals.
     */
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
            if (totalBytes <= 0) return; // Can't calculate progress without total size
            
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
