package com.distributed.systems;

import com.distributed.systems.shared.service.S3Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generates HTML summary files for completed jobs
 */
public class HtmlSummaryGenerator {

    private static final Logger logger = LoggerFactory.getLogger(HtmlSummaryGenerator.class);

    private final S3Service s3Service;

    public HtmlSummaryGenerator(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    /**
     * Generates an HTML summary from task results
     */
    public String generateSummary(List<JobTracker.TaskResult> results) {
        StringBuilder html = new StringBuilder();

        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"en\">\n");
        html.append("<head>\n");
        html.append("  <meta charset=\"UTF-8\">\n");
        html.append("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("  <title>Text Analysis Results</title>\n");
        html.append("  <style>\n");
        html.append("    body {\n");
        html.append("      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n");
        html.append("      max-width: 1200px;\n");
        html.append("      margin: 0 auto;\n");
        html.append("      padding: 20px;\n");
        html.append("      background-color: #f5f5f5;\n");
        html.append("    }\n");
        html.append("    h1 {\n");
        html.append("      color: #333;\n");
        html.append("      border-bottom: 2px solid #4CAF50;\n");
        html.append("      padding-bottom: 10px;\n");
        html.append("    }\n");
        html.append("    .result {\n");
        html.append("      background: white;\n");
        html.append("      padding: 15px;\n");
        html.append("      margin: 10px 0;\n");
        html.append("      border-radius: 5px;\n");
        html.append("      box-shadow: 0 2px 5px rgba(0,0,0,0.1);\n");
        html.append("    }\n");
        html.append("    .success {\n");
        html.append("      border-left: 4px solid #4CAF50;\n");
        html.append("    }\n");
        html.append("    .error {\n");
        html.append("      border-left: 4px solid #f44336;\n");
        html.append("    }\n");
        html.append("    .analysis-type {\n");
        html.append("      font-weight: bold;\n");
        html.append("      color: #2196F3;\n");
        html.append("    }\n");
        html.append("    a {\n");
        html.append("      color: #1976D2;\n");
        html.append("      text-decoration: none;\n");
        html.append("    }\n");
        html.append("    a:hover {\n");
        html.append("      text-decoration: underline;\n");
        html.append("    }\n");
        html.append("    .error-msg {\n");
        html.append("      color: #f44336;\n");
        html.append("      font-style: italic;\n");
        html.append("    }\n");
        html.append("    .stats {\n");
        html.append("      background: #e3f2fd;\n");
        html.append("      padding: 10px 15px;\n");
        html.append("      border-radius: 5px;\n");
        html.append("      margin-bottom: 20px;\n");
        html.append("    }\n");
        html.append("  </style>\n");
        html.append("</head>\n");
        html.append("<body>\n");
        html.append("  <h1>Text Analysis Results</h1>\n");

        // Statistics
        long successCount = results.stream().filter(r -> r.success).count();
        long errorCount = results.size() - successCount;

        html.append("  <div class=\"stats\">\n");
        html.append("    <strong>Total: ").append(results.size()).append("</strong> | ");
        html.append("    <span style=\"color: #4CAF50;\">Success: ").append(successCount).append("</span> | ");
        html.append("    <span style=\"color: #f44336;\">Errors: ").append(errorCount).append("</span>\n");
        html.append("  </div>\n");

        // Results
        for (JobTracker.TaskResult result : results) {
            if (result.success) {
                // Extract key from s3:// URL if necessary
                String key = result.outputS3Url;
                String bucketAgnosticPrefix = "s3://" + s3Service.getBucketName() + "/";
                if (key != null && key.startsWith(bucketAgnosticPrefix)) {
                    key = key.substring(bucketAgnosticPrefix.length());
                }

                // Generate presigned URL for public access
                String presignedUrl = s3Service.generatePresignedUrl(key);

                html.append("  <div class=\"result success\">\n");
                html.append("    <span class=\"analysis-type\">").append(escapeHtml(result.parsingMethod))
                        .append("</span>: ");
                html.append("    <a href=\"").append(escapeHtml(result.url)).append("\" target=\"_blank\">");
                html.append(escapeHtml(result.url)).append("</a> ");
                html.append("    <a href=\"").append(escapeHtml(presignedUrl)).append("\" target=\"_blank\">");
                html.append("[Output]</a>\n");
                html.append("  </div>\n");
            } else {
                html.append("  <div class=\"result error\">\n");
                html.append("    <span class=\"analysis-type\">").append(escapeHtml(result.parsingMethod))
                        .append("</span>: ");
                html.append("    <a href=\"").append(escapeHtml(result.url)).append("\" target=\"_blank\">");
                html.append(escapeHtml(result.url)).append("</a> ");
                html.append("    <span class=\"error-msg\">").append(escapeHtml(result.error)).append("</span>\n");
                html.append("  </div>\n");
            }
        }

        html.append("</body>\n");
        html.append("</html>\n");

        logger.info("Generated HTML summary with {} results ({} success, {} errors)",
                results.size(), successCount, errorCount);

        return html.toString();
    }

    /**
     * Escapes HTML special characters
     */
    private String escapeHtml(String text) {
        if (text == null) {
            return "";
        }
        return text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
