package com.distributed.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generates HTML summary files for completed jobs
 */
public class HtmlSummaryGenerator {

        private static final Logger logger = LoggerFactory.getLogger(HtmlSummaryGenerator.class);

        // No constructor needed - fully static

        /**
         * Generates an HTML summary from task results
         */
        /**
         * Generates an HTML summary from task results
         */
        public static String generateHtml(List<com.distributed.systems.shared.model.TaskResultMetadata> results) {
                StringBuilder html = new StringBuilder();

                html.append("<!DOCTYPE html>\n");
                html.append("<html lang=\"en\">\n");
                html.append("<head>\n");
                // ... (styles omitted for brevity if unchanged, but for safety I will include
                // minimal or full)
                // actually sticking to the existing style block logic is fine but I need to
                // make sure I don't lose the styles
                // I will copy the styles from my view of the file.
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

                long successCount = results.stream().filter(r -> r.isSuccess()).count();
                long errorCount = results.size() - successCount;

                html.append("  <div class=\"stats\">\n");
                html.append("    <strong>Total: ").append(results.size()).append("</strong> | ");
                html.append("    <span style=\"color: #4CAF50;\">Success: ").append(successCount).append("</span> | ");
                html.append("    <span style=\"color: #f44336;\">Errors: ").append(errorCount).append("</span>\n");
                html.append("  </div>\n");

                // Results
                for (com.distributed.systems.shared.model.TaskResultMetadata result : results) {
                        if (result.isSuccess()) {
                                // For S3 output URL, we might want to just show the link.
                                // Since this runs in Manager, we might not have S3Service instance to presign
                                // if static method.
                                // But the user requested generic structure.
                                // Let's just link to the output URL provided in metadata.

                                html.append("  <div class=\"result success\">\n");
                                html.append("    <span class=\"analysis-type\">")
                                                .append(escapeHtml(result.getParsingMethod()))
                                                .append("</span>: ");
                                html.append("    <a href=\"").append(escapeHtml(result.getOriginalUrl()))
                                                .append("\" target=\"_blank\">");
                                html.append(escapeHtml(result.getOriginalUrl())).append("</a> ");

                                // Convert s3://bucket/key to AWS Console Link
                                String s3Url = result.getOutputS3Url();
                                String consoleLink = s3Url;
                                if (s3Url != null && s3Url.startsWith("s3://")) {
                                        String[] parts = s3Url.substring(5).split("/", 2);
                                        if (parts.length == 2) {
                                                String bucket = parts[0];
                                                String key = parts[1];
                                                // https://s3.console.aws.amazon.com/s3/object/BUCKET?region=REGION&prefix=KEY
                                                // We might not have region here easily if static, but usually us-east-1
                                                // default or just object URL
                                                // Safer generic object URL: https://BUCKET.s3.amazonaws.com/KEY (if
                                                // public)
                                                // Or Console:
                                                // https://s3.console.aws.amazon.com/s3/object/bucket?prefix=key
                                                consoleLink = "https://s3.console.aws.amazon.com/s3/object/" + bucket
                                                                + "?prefix=" + key;
                                        }
                                }

                                html.append("    <a href=\"").append(escapeHtml(consoleLink))
                                                .append("\" target=\"_blank\">");
                                html.append("[Output]</a>\n");
                                html.append("  </div>\n");
                        } else {
                                html.append("  <div class=\"result error\">\n");
                                html.append("    <span class=\"analysis-type\">")
                                                .append(escapeHtml(result.getParsingMethod()))
                                                .append("</span>: ");
                                html.append("    <a href=\"").append(escapeHtml(result.getOriginalUrl()))
                                                .append("\" target=\"_blank\">");
                                html.append(escapeHtml(result.getOriginalUrl())).append("</a> ");
                                html.append("    <span class=\"error-msg\">")
                                                .append(escapeHtml(result.getErrorMessage()))
                                                .append("</span>\n");
                                html.append("  </div>\n");
                        }
                }

                html.append("</body>\n");
                html.append("</html>\n");

                logger.info("Generated HTML summary with {} results ({} success, {} errors)",
                                results.size(), successCount, errorCount);

                return html.toString();
        }

        // Changing instance to static requires changing the call site or keeping
        // instance and delegating?
        // WorkerResultsListener calls static
        // HtmlSummaryGenerator.generateHtml(aggregatedResults);
        // So this method should be static.

        /**
         * Escapes HTML special characters
         */
        private static String escapeHtml(String text) {
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
