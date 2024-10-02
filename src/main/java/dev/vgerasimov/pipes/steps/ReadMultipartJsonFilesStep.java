package dev.vgerasimov.pipes.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

public class ReadMultipartJsonFilesStep extends BaseStep implements Step {

  private final ObjectMapper objectMapper = new ObjectMapper();

  protected ReadMultipartJsonFilesStep(String name) {
    super(name);
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    if (payload == null) {
      log.error("Payload is null");
      return Flux.empty();
    }
    if (payload.isEmpty()) {
      log.warn("Payload is empty");
      return Flux.just(payload);
    }

    var files = (Collection<MultipartFile>) payload.get("files");
    if (files == null) {
      log.warn("Payload doesn't contain files");
      return Flux.empty();
    }

    ArrayList<Map<String, Object>> result = new ArrayList<>();

    for (var file : files) {
      var contentType = file.getContentType();
      if (contentType == null) {
        log.warn("Payload doesn't contain file content type");
        contentType = MediaType.APPLICATION_JSON_VALUE;
      }

      if (!MediaType.APPLICATION_JSON.includes(MediaType.parseMediaType(contentType))) {
        log.warn("Payload file content type is not JSON");
        return Flux.empty();
      }

      try {
        var content = file.getBytes();
        var map = objectMapper.readValue(content, Map.class);
        result.add(map);
      } catch (IOException e) {
        log.error("Cannot read file", e);
      }
    }

    return Flux.fromIterable(result);
  }
}
