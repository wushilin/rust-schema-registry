package oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.CompatibilityChecker;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.File;
import java.util.*;

/**
 * Runs a schema corpus through Confluent's own schema libraries (the code the
 * Schema Registry server executes) and writes the results as golden data:
 *
 *   java oracle.Oracle tests/golden/corpus.json tests/golden/golden.json
 *
 * For each schema: is it accepted for registration, and what canonical and
 * normalized strings does the server store. For each compatibility pair: the
 * messages returned by BACKWARD and FORWARD checks (empty = compatible).
 */
public class Oracle {
  static final ObjectMapper M = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  static List<SchemaReference> refs(JsonNode n) {
    List<SchemaReference> out = new ArrayList<>();
    if (n != null) for (JsonNode r : n) out.add(new SchemaReference(r.get("name").asText(), r.path("subject").asText(r.get("name").asText()), r.path("version").asInt(1)));
    return out;
  }

  static Map<String, String> resolved(JsonNode n) {
    Map<String, String> out = new LinkedHashMap<>();
    if (n != null) for (JsonNode r : n) out.put(r.get("name").asText(), r.get("schema").asText());
    return out;
  }

  /** Same path as the server registering a new schema: construct (isNew) then validate. */
  static ParsedSchema parse(String type, String schema, JsonNode refNodes) {
    List<SchemaReference> refs = refs(refNodes);
    Map<String, String> res = resolved(refNodes);
    ParsedSchema s;
    switch (type) {
      case "AVRO": s = new AvroSchema(schema, refs, res, null, null, null, true); break;
      case "JSON": s = new JsonSchema(schema, refs, res, null); break;
      case "PROTOBUF": s = new ProtobufSchema(schema, refs, res, null, null); break;
      default: throw new IllegalArgumentException("type " + type);
    }
    s.validate(false);
    return s;
  }

  public static void main(String[] args) throws Exception {
    JsonNode corpus = M.readTree(new File(args[0]));
    ObjectNode out = M.createObjectNode();

    ObjectNode schemas = out.putObject("schemas");
    for (JsonNode c : corpus.path("schemas")) {
      ObjectNode r = schemas.putObject(c.get("id").asText());
      try {
        ParsedSchema s = parse(c.get("type").asText(), c.get("schema").asText(), c.get("refs"));
        r.put("valid", true);
        r.put("canonical", s.canonicalString());
        r.put("normalized", s.normalize().canonicalString());
      } catch (Throwable e) {
        r.put("valid", false);
        r.put("error", String.valueOf(e.getMessage()));
      }
    }

    ObjectNode compat = out.putObject("compat");
    for (JsonNode c : corpus.path("compat")) {
      ObjectNode r = compat.putObject(c.get("id").asText());
      String type = c.get("type").asText();
      try {
        ParsedSchema oldS = parse(type, c.get("old").asText(), c.get("old_refs"));
        ParsedSchema newS = parse(type, c.get("new").asText(), c.get("new_refs"));
        for (String level : List.of("BACKWARD", "FORWARD")) {
          CompatibilityChecker checker = level.equals("BACKWARD") ? CompatibilityChecker.BACKWARD_CHECKER : CompatibilityChecker.FORWARD_CHECKER;
          ArrayNode msgs = r.putArray(level);
          for (String m : checker.isCompatible(newS, List.of(oldS))) msgs.add(m);
        }
      } catch (Throwable e) {
        r.put("error", String.valueOf(e.getMessage()));
      }
    }
    // Version chains: the last version checked against the earlier ones
    // (oldest first, as the server passes them) at every level.
    ObjectNode chains = out.putObject("chains");
    for (JsonNode c : corpus.path("chains")) {
      ObjectNode r = chains.putObject(c.get("id").asText());
      try {
        List<ParsedSchema> versions = new ArrayList<>();
        for (int i = 0; i < c.get("versions").size(); i++) {
          ParsedSchema s = parse(c.get("types").get(i).asText(), c.get("versions").get(i).asText(), null);
          versions.add(s.copy(i + 1));
        }
        ParsedSchema newest = versions.get(versions.size() - 1);
        List<ParsedSchema> previous = versions.subList(0, versions.size() - 1);
        for (io.confluent.kafka.schemaregistry.CompatibilityLevel level : io.confluent.kafka.schemaregistry.CompatibilityLevel.values()) {
          ArrayNode msgs = r.putArray(level.name());
          for (String m : CompatibilityChecker.checker(level).isCompatible(newest, previous)) msgs.add(m);
        }
      } catch (Throwable e) {
        r.put("error", String.valueOf(e.getMessage()));
      }
    }
    M.writeValue(new File(args[1]), out);
    System.out.println("wrote " + args[1] + ": " + schemas.size() + " schemas, " + compat.size() + " compat pairs, " + chains.size() + " chains");
  }
}
