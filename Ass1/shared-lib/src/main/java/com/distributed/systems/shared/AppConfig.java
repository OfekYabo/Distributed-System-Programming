package com.distributed.systems.shared;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Shared configuration handler that loads from both Environment Variables and
 * .env file.
 * Simplifies configuration management by providing direct access to typed
 * values.
 */
public class AppConfig {

    private final Dotenv dotenv;

    public AppConfig() {
        // Load .env file if present, otherwise ignore
        this.dotenv = Dotenv.configure().ignoreIfMissing().load();
    }

    /**
     * Get a string value from environment.
     * Throws RuntimeException if not found.
     */
    public String getString(String key) {
        String value = get(key);
        if (value == null) {
            throw new RuntimeException("Missing required environment variable: " + key);
        }
        return value;
    }

    /**
     * Get a string value, or default if missing.
     */
    public String getOptional(String key, String defaultValue) {
        String value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Get an int value from environment.
     * Throws RuntimeException if not found or not a valid integer.
     */
    public int getInt(String key) {
        String value = getString(key);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer for environment variable: " + key + ", value: " + value);
        }
    }

    /**
     * Get an int value, or default if missing.
     */
    public int getIntOptional(String key, int defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Helper to look up in System Env first, then .env file
     */
    private String get(String key) {
        // 1. Check System Env (Standard in Cloud/Docker/EC2)
        String value = System.getenv(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        // 2. Check .env file (Local development)
        return dotenv.get(key);
    }
}
