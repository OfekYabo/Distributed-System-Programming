package com.distributed.systems;

import com.distributed.systems.model.WorkerTaskMessage;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.nndep.DependencyParser;
import edu.stanford.nlp.parser.shiftreduce.ShiftReduceParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Processes text analysis tasks
 * Downloads files, performs parsing, and handles errors
 */
public class TaskProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskProcessor.class);
    
    private final WorkerConfig config;
    private final S3Uploader s3Uploader;
    
    // Stanford NLP models - initialized lazily and reused
    private static MaxentTagger posTagger;
    private static MaxentTagger constituencyTagger;
    private static ShiftReduceParser constituencyParser;
    private static MaxentTagger dependencyTagger;
    private static DependencyParser dependencyParser;
    
    public TaskProcessor(WorkerConfig config, S3Uploader s3Uploader) {
        this.config = config;
        this.s3Uploader = s3Uploader;
        
        // Ensure temp directory exists
        createTempDirectory();
    }
    
    /**
     * Creates the temporary directory if it doesn't exist
     */
    private void createTempDirectory() {
        try {
            Path tempDir = Paths.get(config.getTempDirectory());
            if (!Files.exists(tempDir)) {
                Files.createDirectories(tempDir);
                logger.info("Created temp directory: {}", tempDir);
            }
        } catch (Exception e) {
            logger.error("Failed to create temp directory", e);
            throw new RuntimeException("Failed to create temp directory", e);
        }
    }
    
    /**
     * Processes a task: downloads file, performs parsing, uploads result
     * 
     * @param taskData The task data containing URL and parsing method
     * @return The S3 URL of the processed file
     * @throws Exception if processing fails
     */
    public String processTask(WorkerTaskMessage.TaskData taskData) throws Exception {
        String url = taskData.getUrl();
        String parsingMethod = taskData.getParsingMethod();
        
        logger.info("Processing task: URL={}, Method={}", url, parsingMethod);
        
        // Download the file
        File downloadedFile = downloadFile(url);
        
        try {
            // Parse the file (placeholder for now)
            File parsedFile = parseFile(downloadedFile, parsingMethod);
            
            try {
                // Upload to S3
                String s3Url = s3Uploader.uploadFile(parsedFile, url, parsingMethod);
                logger.info("Task processed successfully. S3 URL: {}", s3Url);
                
                return s3Url;
            } finally {
                // Clean up parsed file
                if (parsedFile != null && parsedFile.exists()) {
                    parsedFile.delete();
                }
            }
        } finally {
            // Clean up downloaded file
            if (downloadedFile != null && downloadedFile.exists()) {
                downloadedFile.delete();
            }
        }
    }
    
    /**
     * Downloads a file from URL using wget
     * 
     * @param url The URL to download from
     * @return The downloaded file
     * @throws Exception if download fails
     */
    private File downloadFile(String url) throws Exception {
        String filename = "input_" + System.currentTimeMillis() + ".txt";
        File outputFile = new File(config.getTempDirectory(), filename);
        
        logger.info("Downloading file from URL: {}", url);
        
        try {
            // Use wget to download the file
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "wget",
                    "-O", outputFile.getAbsolutePath(),
                    "--timeout=30",
                    "--tries=3",
                    url
            );
            
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            
            // Read output for logging
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.debug("wget: {}", line);
                }
            }
            
            // Wait for completion with timeout
            boolean completed = process.waitFor(60, TimeUnit.SECONDS);
            
            if (!completed) {
                process.destroyForcibly();
                throw new Exception("Download timeout exceeded");
            }
            
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new Exception("wget failed with exit code: " + exitCode);
            }
            
            if (!outputFile.exists() || outputFile.length() == 0) {
                throw new Exception("Downloaded file is empty or does not exist");
            }
            
            logger.info("File downloaded successfully: {} ({} bytes)", 
                    outputFile.getName(), outputFile.length());
            
            return outputFile;
            
        } catch (Exception e) {
            // Clean up on failure
            if (outputFile.exists()) {
                outputFile.delete();
            }
            throw new Exception("Failed to download file from URL: " + url + " - " + e.getMessage(), e);
        }
    }
    
    /**
     * Parses a text file using Stanford NLP
     * 
     * @param inputFile The file to parse
     * @param parsingMethod The parsing method (POS, CONSTITUENCY, DEPENDENCY)
     * @return The parsed file
     * @throws Exception if parsing fails
     */
    private File parseFile(File inputFile, String parsingMethod) throws Exception {
        String outputFilename = "parsed_" + System.currentTimeMillis() + ".txt";
        File outputFile = new File(config.getTempDirectory(), outputFilename);
        
        logger.info("Parsing file with method: {}", parsingMethod);
        
        try {
            switch (parsingMethod) {
                case "POS":
                    performPosTagging(inputFile, outputFile);
                    break;
                case "CONSTITUENCY":
                    performConstituencyParsing(inputFile, outputFile);
                    break;
                case "DEPENDENCY":
                    performDependencyParsing(inputFile, outputFile);
                    break;
                default:
                    throw new Exception("Unknown parsing method: " + parsingMethod);
            }
            
            logger.info("File parsed successfully with method {}: {}", parsingMethod, outputFile.getName());
            return outputFile;
            
        } catch (Exception e) {
            // Clean up on failure
            if (outputFile.exists()) {
                outputFile.delete();
            }
            throw new Exception("Failed to parse file with method " + parsingMethod + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Performs POS (Part-of-Speech) tagging on the input file
     */
    private void performPosTagging(File inputFile, File outputFile) throws Exception {
        logger.info("Performing POS tagging");
        
        // Initialize POS tagger lazily
        if (posTagger == null) {
            synchronized (TaskProcessor.class) {
                if (posTagger == null) {
                    logger.info("Loading POS tagger model...");
                    posTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
                    logger.info("POS tagger model loaded");
                }
            }
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Process sentence by sentence for better memory efficiency
            DocumentPreprocessor dp = new DocumentPreprocessor(new FileReader(inputFile));
            
            for (List<HasWord> sentence : dp) {
                if (sentence.isEmpty()) {
                    continue;
                }
                
                try {
                    List<TaggedWord> taggedSentence = posTagger.tagSentence(sentence);
                    
                    for (TaggedWord word : taggedSentence) {
                        writer.write(word.word() + "/" + word.tag() + " ");
                    }
                    writer.write("\n");
                } catch (Exception e) {
                    logger.warn("Failed to tag sentence, skipping: {}", e.getMessage());
                    writer.write("[POS TAGGING ERROR: " + e.getMessage() + "]\n");
                }
            }
        }
    }
    
    /**
     * Performs constituency parsing on the input file using ShiftReduceParser
     * Note: ShiftReduceParser requires pre-tagged text
     */
    private void performConstituencyParsing(File inputFile, File outputFile) throws Exception {
        logger.info("Performing constituency parsing");
        
        // Initialize tagger and parser lazily
        if (constituencyTagger == null || constituencyParser == null) {
            synchronized (TaskProcessor.class) {
                if (constituencyTagger == null) {
                    logger.info("Loading constituency tagger model...");
                    constituencyTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
                    logger.info("Constituency tagger model loaded");
                }
                if (constituencyParser == null) {
                    logger.info("Loading constituency parser model...");
                    constituencyParser = ShiftReduceParser.loadModel("edu/stanford/nlp/models/srparser/englishSR.ser.gz");
                    logger.info("Constituency parser model loaded");
                }
            }
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Process sentence by sentence for better memory efficiency
            DocumentPreprocessor dp = new DocumentPreprocessor(new FileReader(inputFile));
            
            for (List<HasWord> sentence : dp) {
                if (sentence.isEmpty()) {
                    continue;
                }
                
                try {
                    // First tag the sentence
                    List<TaggedWord> tagged = constituencyTagger.tagSentence(sentence);
                    // Then parse the tagged sentence
                    Tree parseTree = constituencyParser.apply(tagged);
                    writer.write(parseTree.toString());
                    writer.write("\n\n");
                } catch (Exception e) {
                    logger.warn("Failed to parse sentence, skipping: {}", e.getMessage());
                    writer.write("[PARSE ERROR: " + e.getMessage() + "]\n\n");
                }
            }
        }
    }
    
    /**
     * Performs dependency parsing on the input file using Neural Network Dependency Parser
     * Note: DependencyParser requires pre-tagged text
     */
    private void performDependencyParsing(File inputFile, File outputFile) throws Exception {
        logger.info("Performing dependency parsing");
        
        // Initialize tagger and parser lazily
        if (dependencyTagger == null || dependencyParser == null) {
            synchronized (TaskProcessor.class) {
                if (dependencyTagger == null) {
                    logger.info("Loading dependency tagger model...");
                    dependencyTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
                    logger.info("Dependency tagger model loaded");
                }
                if (dependencyParser == null) {
                    logger.info("Loading dependency parser model...");
                    dependencyParser = DependencyParser.loadFromModelFile(DependencyParser.DEFAULT_MODEL);
                    logger.info("Dependency parser model loaded");
                }
            }
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Process sentence by sentence for better memory efficiency
            DocumentPreprocessor dp = new DocumentPreprocessor(new FileReader(inputFile));
            
            for (List<HasWord> sentence : dp) {
                if (sentence.isEmpty()) {
                    continue;
                }
                
                try {
                    // First tag the sentence
                    List<TaggedWord> tagged = dependencyTagger.tagSentence(sentence);
                    // Then parse the tagged sentence
                    GrammaticalStructure gs = dependencyParser.predict(tagged);
                    
                    // Write the dependencies
                    writer.write(gs.toString());
                    writer.write("\n\n");
                } catch (Exception e) {
                    logger.warn("Failed to parse sentence dependencies, skipping: {}", e.getMessage());
                    writer.write("[PARSE ERROR: " + e.getMessage() + "]\n\n");
                }
            }
        }
    }
    
    /**
     * Validates the parsing method
     */
    public static boolean isValidParsingMethod(String method) {
        return "POS".equals(method) || 
               "CONSTITUENCY".equals(method) || 
               "DEPENDENCY".equals(method);
    }
}

