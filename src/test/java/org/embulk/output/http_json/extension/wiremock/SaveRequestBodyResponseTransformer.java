package org.embulk.output.http_json.extension.wiremock;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SaveRequestBodyResponseTransformer extends ResponseTransformer {

    public static final String NAME = "save-request-body";
    public static final String OUTPUT_FILE_PATH_PARAMETER = "output_file_path";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Response transform(
            Request request, Response response, FileSource files, Parameters parameters) {
        String outputFilePath = parameters.getString(OUTPUT_FILE_PATH_PARAMETER);
        if (outputFilePath == null) {
            throw new IllegalArgumentException("output_file_path is required");
        }
        try {
            Files.write(
                    Paths.get(outputFilePath),
                    (request.getBodyAsString() + System.lineSeparator()).getBytes(),
                    CREATE,
                    APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    @Override
    public boolean applyGlobally() {
        return false;
    }
}
