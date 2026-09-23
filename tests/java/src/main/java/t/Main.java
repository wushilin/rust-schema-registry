package t;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * Integration suite driving the registry with Confluent's official Java
 * client and serializers (kafka-*-serializer 7.9). No Kafka broker is needed:
 * serializers produce the Confluent wire format (magic byte, schema id,
 * payload), deserializers resolve schema ids through the registry.
 *
 *   java -cp target/classes:$(cat cp.txt) t.Main http://127.0.0.1:8081 [user:password]
 *
 * Covers, for Avro, Protobuf and JSON Schema: encode/decode round trips,
 * schema references, schema evolution (data written with v1 read through v2
 * and back, with the writer schema fetched from the registry) and the
 * production serializer mode (auto.register.schemas=false, use.latest.version=true).
 * It also passes against a real Confluent Schema Registry.
 */
public class Main {
  static int failed = 0;
  static final ObjectMapper JSON = new ObjectMapper();
  static final List<SchemaProvider> PROVIDERS =
      List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider());
  static String url;
  static Map<String, Object> conf = new HashMap<>();
  static final String RUN = Long.toString(System.nanoTime() % 1_000_000_000L, 36);

  static void check(String label, boolean ok) {
    System.out.println((ok ? "  ok   " : "  FAIL ") + label);
    if (!ok) failed++;
  }

  static void section(String name) {
    System.out.println(name);
  }

  static CachedSchemaRegistryClient client() {
    return new CachedSchemaRegistryClient(List.of(url), 100, PROVIDERS, conf, null);
  }

  static Map<String, Object> serde(Object... kv) {
    Map<String, Object> m = new HashMap<>(conf);
    for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
    return m;
  }

  /** Unique name per run so the suite can be re-run against the same registry. */
  static String n(String base) {
    return base + "-" + RUN;
  }

  /** Raw GET for endpoints the client library doesn't wrap. */
  static String httpGet(String path) throws Exception {
    var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url + path)).GET();
    Object info = conf.get("basic.auth.user.info");
    if (info != null) {
      req.header("Authorization", "Basic " + Base64.getEncoder().encodeToString(info.toString().getBytes()));
    }
    return java.net.http.HttpClient.newHttpClient().send(req.build(), java.net.http.HttpResponse.BodyHandlers.ofString()).body();
  }

  static int idOf(byte[] framed) {
    return ByteBuffer.wrap(framed, 1, 4).getInt();
  }

  static byte[] avroBody(Schema schema, GenericRecord rec) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(rec, enc);
    enc.flush();
    return out.toByteArray();
  }

  /** Resolve writer data into a reader schema, the way an Avro consumer with its own schema does. */
  static GenericRecord avroRead(byte[] framed, Schema reader, CachedSchemaRegistryClient sr) throws Exception {
    Schema writer = ((AvroSchema) sr.getSchemaById(idOf(framed))).rawSchema();
    return new GenericDatumReader<GenericRecord>(writer, reader)
        .read(null, DecoderFactory.get().binaryDecoder(framed, 5, framed.length - 5, null));
  }

  public static void main(String[] a) throws Exception {
    url = a[0];
    conf.put("schema.registry.url", url);
    if (a.length > 1) {
      conf.put("basic.auth.credentials.source", "USER_INFO");
      conf.put("basic.auth.user.info", a[1]);
    }
    CachedSchemaRegistryClient sr = client();

    avro(sr);
    avroEvolution(sr);
    avroReferences(sr);
    protobuf(sr);
    protobufEvolution(sr);
    protobufReferences(sr);
    json(sr);
    jsonEvolution(sr);
    jsonReferences(sr);
    contexts(sr);
    deletes(sr);

    System.out.println(failed == 0 ? "\njava client checks passed" : "\n" + failed + " FAILED");
    System.exit(failed == 0 ? 0 : 1);
  }

  // ------------------------------------------------------------------ Avro

  static void avro(CachedSchemaRegistryClient sr) throws Exception {
    section("avro: serde and registry API");
    String topic = n("jusers");
    AvroSchema user = new AvroSchema("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"j\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");
    GenericRecord rec = new GenericData.Record(user.rawSchema());
    rec.put("name", "bob");
    byte[] bytes = new KafkaAvroSerializer(sr, conf).serialize(topic, rec);
    int id = idOf(bytes);
    check("serializer auto-registers and frames (magic 0)", bytes[0] == 0 && id > 0);
    GenericRecord back = (GenericRecord) new KafkaAvroDeserializer(client(), conf).deserialize(topic, bytes);
    check("round trip through a fresh client", back.get("name").toString().equals("bob"));
    String subject = topic + "-value";
    check("getLatestSchemaMetadata", sr.getLatestSchemaMetadata(subject).getId() == id);
    check("getId (lookup)", sr.getId(subject, user) == id);
    check("getVersion", sr.getVersion(subject, user) == 1);
    check("getAllVersions", sr.getAllVersions(subject).equals(List.of(1)));
    check("getAllSubjects", sr.getAllSubjects().contains(subject));
    AvroSchema bad = new AvroSchema("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"j\",\"fields\":[{\"name\":\"name\",\"type\":\"int\"}]}");
    check("testCompatibility false", !sr.testCompatibility(subject, bad));
    check("testCompatibilityVerbose has messages", !sr.testCompatibilityVerbose(subject, bad).isEmpty());
    try {
      sr.register(subject, bad);
      check("incompatible registration rejected", false);
    } catch (RestClientException e) {
      check("incompatible registration rejected with 409", e.getErrorCode() == 409);
    }
    sr.updateCompatibility(subject, "FULL");
    check("updateCompatibility / getCompatibility", sr.getCompatibility(subject).equals("FULL"));
    sr.setMode("READONLY", subject);
    check("setMode / getMode", sr.getMode(subject).equals("READONLY"));
    sr.deleteMode(subject);
  }

  static void avroEvolution(CachedSchemaRegistryClient sr) throws Exception {
    section("avro: evolution v1 -> v2 (field added with default)");
    String topic = n("javro-evo");
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Ev\",\"namespace\":\"j\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Ev\",\"namespace\":\"j\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\",\"default\":\"n/a\"}]}");
    GenericRecord r1 = new GenericData.Record(v1);
    r1.put("id", 7L);
    byte[] b1 = new KafkaAvroSerializer(sr, conf).serialize(topic, r1);
    int id2 = sr.register(topic + "-value", new AvroSchema(v2));
    check("v2 registered as a new version", id2 != idOf(b1) && sr.getAllVersions(topic + "-value").equals(List.of(1, 2)));

    GenericRecord newReader = avroRead(b1, v2, sr);
    check("v1 data read with v2 schema: default fills the new field", newReader.get("id").equals(7L) && newReader.get("name").toString().equals("n/a"));

    // Production mode: no auto-registration, always write with the latest registered schema.
    GenericRecord r2 = new GenericData.Record(v2);
    r2.put("id", 8L);
    r2.put("name", "eve");
    byte[] b2 = new KafkaAvroSerializer(sr, serde("auto.register.schemas", false, "use.latest.version", true)).serialize(topic, r2);
    check("use.latest.version writes with v2's id", idOf(b2) == id2);
    GenericRecord oldReader = avroRead(b2, v1, sr);
    check("v2 data read with v1 schema: new field ignored", oldReader.get("id").equals(8L) && oldReader.getSchema().getField("name") == null);
    GenericRecord generic = (GenericRecord) new KafkaAvroDeserializer(client(), conf).deserialize(topic, b2);
    check("deserializer returns v2 data with the writer schema", generic.get("name").toString().equals("eve"));

    try {
      new KafkaAvroSerializer(sr, serde("auto.register.schemas", false)).serialize(n("javro-unregistered"), r1);
      check("auto.register=false with an unknown schema fails", false);
    } catch (Exception e) {
      check("auto.register=false with an unknown schema fails", true);
    }
  }

  static void avroReferences(CachedSchemaRegistryClient sr) throws Exception {
    section("avro: references");
    String addrSubject = n("jaddr");
    String addr = "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"acme\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}";
    sr.register(addrSubject, new AvroSchema(addr));
    String person = "{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"acme\",\"fields\":[{\"name\":\"home\",\"type\":\"acme.Address\"}]}";
    AvroSchema personSchema = new AvroSchema(person, List.of(new SchemaReference("acme.Address", addrSubject, 1)), Map.of("acme.Address", addr), null);
    String topic = n("jperson");
    int pid = sr.register(topic + "-value", personSchema);
    AvroSchema fetched = (AvroSchema) client().getSchemaById(pid);
    check("schema by id carries its reference", fetched.references().size() == 1 && fetched.references().get(0).getSubject().equals(addrSubject));

    GenericRecord home = new GenericData.Record(personSchema.rawSchema().getField("home").schema());
    home.put("street", "Main St");
    GenericRecord p = new GenericData.Record(personSchema.rawSchema());
    p.put("home", home);
    byte[] bytes = new KafkaAvroSerializer(sr, serde("auto.register.schemas", false, "use.latest.version", true)).serialize(topic, p);
    GenericRecord back = (GenericRecord) new KafkaAvroDeserializer(client(), conf).deserialize(topic, bytes);
    check("round trip through a referenced type", ((GenericRecord) back.get("home")).get("street").toString().equals("Main St"));
    JsonNode refBy = JSON.readTree(httpGet("/subjects/" + addrSubject + "/versions/1/referencedby"));
    check("referencedby lists the referrer", refBy.isArray() && refBy.toString().contains(Integer.toString(pid)));
  }

  // ------------------------------------------------------------------ Protobuf

  static DynamicMessage.Builder builder(ProtobufSchema s) {
    return DynamicMessage.newBuilder(s.toDescriptor());
  }

  static void protobuf(CachedSchemaRegistryClient sr) throws Exception {
    section("protobuf: serde (text schemas, well-known imports, maps, oneofs)");
    ProtobufSchema ps = new ProtobufSchema("syntax = \"proto3\";\npackage j;\nimport \"google/protobuf/timestamp.proto\";\n"
        + "message Ev { string id = 1; google.protobuf.Timestamp at = 2; map<string,int32> m = 3; oneof k { string s = 4; int64 n = 5; } }\n");
    Descriptor d = ps.toDescriptor();
    DynamicMessage msg = builder(ps).setField(d.findFieldByName("id"), "e1").setField(d.findFieldByName("n"), 7L).build();
    String topic = n("jevents");
    byte[] pb = new KafkaProtobufSerializer<DynamicMessage>(sr, conf).serialize(topic, msg);
    DynamicMessage back = new KafkaProtobufDeserializer<DynamicMessage>(client(), conf).deserialize(topic, pb);
    Descriptor bd = back.getDescriptorForType();
    check("round trip", back.getField(bd.findFieldByName("id")).equals("e1") && back.getField(bd.findFieldByName("n")).equals(7L));
    check("stored schema re-parses in Java", client().getSchemaById(idOf(pb)) instanceof ProtobufSchema);
  }

  static void protobufEvolution(CachedSchemaRegistryClient sr) throws Exception {
    section("protobuf: evolution v1 -> v2 (field added)");
    String topic = n("jproto-evo");
    ProtobufSchema v1 = new ProtobufSchema("syntax = \"proto3\";\npackage j;\nmessage Evo { string id = 1; }\n");
    ProtobufSchema v2 = new ProtobufSchema("syntax = \"proto3\";\npackage j;\nmessage Evo { string id = 1; int32 qty = 2; }\n");
    byte[] b1 = new KafkaProtobufSerializer<DynamicMessage>(sr, conf)
        .serialize(topic, builder(v1).setField(v1.toDescriptor().findFieldByName("id"), "a").build());
    int id2 = sr.register(topic + "-value", v2);
    check("v2 registered as a new version", id2 != idOf(b1));
    // Payload after the 5-byte header and the message-index list (a single 0 for the first message).
    DynamicMessage asV2 = DynamicMessage.parseFrom(v2.toDescriptor(), Arrays.copyOfRange(b1, 6, b1.length));
    check("v1 data read with v2: new field defaults", asV2.getField(v2.toDescriptor().findFieldByName("id")).equals("a")
        && !asV2.hasField(v2.toDescriptor().findFieldByName("qty")));

    Descriptor d2 = v2.toDescriptor();
    byte[] b2 = new KafkaProtobufSerializer<DynamicMessage>(sr, serde("auto.register.schemas", false, "use.latest.version", true))
        .serialize(topic, DynamicMessage.newBuilder(d2).setField(d2.findFieldByName("id"), "b").setField(d2.findFieldByName("qty"), 3).build());
    check("use.latest.version writes with v2's id", idOf(b2) == id2);
    DynamicMessage asV1 = DynamicMessage.parseFrom(v1.toDescriptor(), Arrays.copyOfRange(b2, 6, b2.length));
    check("v2 data read with v1: known field intact", asV1.getField(v1.toDescriptor().findFieldByName("id")).equals("b"));
    DynamicMessage viaRegistry = new KafkaProtobufDeserializer<DynamicMessage>(client(), conf).deserialize(topic, b2);
    check("deserializer decodes v2 with the writer schema", viaRegistry.getField(viaRegistry.getDescriptorForType().findFieldByName("qty")).equals(3));

    ProtobufSchema bad = new ProtobufSchema("syntax = \"proto3\";\npackage j;\nmessage Evo { int64 id = 1; }\n");
    check("incompatible v3 (string -> int64) rejected by the registry", !sr.testCompatibility(topic + "-value", bad));
  }

  static void protobufReferences(CachedSchemaRegistryClient sr) throws Exception {
    section("protobuf: references (serializer auto-registers the dependency)");
    String money = "syntax = \"proto3\";\npackage acme;\nmessage Money { int64 cents = 1; }\n";
    String depName = "acme/money-" + RUN + ".proto";
    String order = "syntax = \"proto3\";\npackage acme;\nimport \"" + depName + "\";\nmessage Order { string id = 1; Money total = 2; }\n";
    ProtobufSchema os = new ProtobufSchema(order, List.of(new SchemaReference(depName, depName, 1)), Map.of(depName, money), null, null);
    Descriptor od = os.toDescriptor();
    Descriptor md = od.findFieldByName("total").getMessageType();
    DynamicMessage m = DynamicMessage.newBuilder(od)
        .setField(od.findFieldByName("id"), "o-1")
        .setField(od.findFieldByName("total"), DynamicMessage.newBuilder(md).setField(md.findFieldByName("cents"), 1234L).build())
        .build();
    String topic = n("jorders");
    byte[] bytes = new KafkaProtobufSerializer<DynamicMessage>(sr, conf).serialize(topic, m);
    check("dependency registered under its file name", sr.getAllSubjects().contains(depName));
    check("main schema records the reference", !sr.getLatestSchemaMetadata(topic + "-value").getReferences().isEmpty());
    DynamicMessage back = new KafkaProtobufDeserializer<DynamicMessage>(client(), conf).deserialize(topic, bytes);
    DynamicMessage total = (DynamicMessage) back.getField(back.getDescriptorForType().findFieldByName("total"));
    check("round trip through the referenced type", total.getField(total.getDescriptorForType().findFieldByName("cents")).equals(1234L));
  }

  // ------------------------------------------------------------------ JSON Schema

  public static class Item {
    public String sku;
    public int qty;

    @Override
    public boolean equals(Object o) {
      return o instanceof Item i && Objects.equals(sku, i.sku) && qty == i.qty;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sku, qty);
    }
  }

  static void json(CachedSchemaRegistryClient sr) throws Exception {
    section("json schema: serde (POJO with a derived schema)");
    String topic = n("jitems");
    Item item = new Item();
    item.sku = "x1";
    item.qty = 3;
    byte[] bytes = new KafkaJsonSchemaSerializer<Item>(sr, conf).serialize(topic, item);
    check("serializer derives and registers a JSON schema", bytes[0] == 0 && client().getSchemaById(idOf(bytes)) instanceof JsonSchema);
    KafkaJsonSchemaDeserializer<Item> de = new KafkaJsonSchemaDeserializer<>(client(), serde("json.value.type", Item.class.getName()), Item.class);
    check("round trip into the POJO", item.equals(de.deserialize(topic, bytes)));
    KafkaJsonSchemaDeserializer<Object> generic = new KafkaJsonSchemaDeserializer<>(client(), conf);
    Object node = generic.deserialize(topic, bytes);
    check("generic deserialization yields the same fields", JSON.valueToTree(node).get("sku").asText().equals("x1"));
  }

  static void jsonEvolution(CachedSchemaRegistryClient sr) throws Exception {
    section("json schema: evolution v1 -> v2 (closed model, optional property added)");
    String topic = n("jjson-evo");
    String subject = topic + "-value";
    JsonSchema v1 = new JsonSchema("{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"string\"}},\"required\":[\"sku\"],\"additionalProperties\":false}");
    JsonSchema v2 = new JsonSchema("{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"string\"},\"qty\":{\"type\":\"integer\"}},\"required\":[\"sku\"],\"additionalProperties\":false}");
    sr.register(subject, v1);
    check("v2 is BACKWARD compatible", sr.testCompatibility(subject, v2));
    int id2 = sr.register(subject, v2);

    JsonNode d1 = JSON.readTree("{\"sku\":\"a\"}");
    JsonNode d2 = JSON.readTree("{\"sku\":\"b\",\"qty\":2}");
    // Writer v1 via an envelope (explicit schema, no auto-registration: must be found by lookup).
    byte[] b1 = new KafkaJsonSchemaSerializer<JsonNode>(sr, serde("auto.register.schemas", false, "json.fail.invalid.schema", true))
        .serialize(topic, JsonSchemaUtils.envelope(v1, d1));
    check("envelope serializer finds v1 by lookup", idOf(b1) != id2);
    JsonSchema latest = (JsonSchema) client().getSchemaBySubjectAndId(subject, id2);
    boolean v1DataValidUnderV2;
    try {
      latest.validate(d1);
      v1DataValidUnderV2 = true;
    } catch (Exception e) {
      v1DataValidUnderV2 = false;
    }
    check("v1 data is valid under v2 (backward: new readers accept old data)", v1DataValidUnderV2);
    boolean v2DataRejectedByV1;
    try {
      v1.validate(d2);
      v2DataRejectedByV1 = false;
    } catch (Exception e) {
      v2DataRejectedByV1 = true;
    }
    check("v2 data is rejected by closed v1 (why FORWARD would fail)", v2DataRejectedByV1);

    // Raw JsonNode payloads carry no schema of their own, hence latest.compatibility.strict=false.
    byte[] b2 = new KafkaJsonSchemaSerializer<JsonNode>(sr, serde("auto.register.schemas", false, "use.latest.version", true, "latest.compatibility.strict", false, "json.fail.invalid.schema", true))
        .serialize(topic, d2);
    check("use.latest.version writes with v2's id", idOf(b2) == id2);
    Object back = new KafkaJsonSchemaDeserializer<Object>(client(), serde("json.fail.invalid.schema", true)).deserialize(topic, b2);
    check("deserializer validates against the writer schema and decodes", JSON.valueToTree(back).get("qty").asInt() == 2);
    try {
      new KafkaJsonSchemaSerializer<JsonNode>(sr, serde("auto.register.schemas", false, "use.latest.version", true, "latest.compatibility.strict", false, "json.fail.invalid.schema", true))
          .serialize(topic, JSON.readTree("{\"sku\":1}"));
      check("serializer rejects data invalid for the latest schema", false);
    } catch (Exception e) {
      check("serializer rejects data invalid for the latest schema", true);
    }
  }

  static void jsonReferences(CachedSchemaRegistryClient sr) throws Exception {
    section("json schema: references");
    String customerSubject = n("jcustomer");
    String customer = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"required\":[\"id\"]}";
    sr.register(customerSubject, new JsonSchema(customer));
    String order = "{\"type\":\"object\",\"properties\":{\"c\":{\"$ref\":\"customer.json\"}},\"required\":[\"c\"]}";
    JsonSchema os = new JsonSchema(order, List.of(new SchemaReference("customer.json", customerSubject, 1)), Map.of("customer.json", customer), null);
    int id = sr.register(n("jorder") + "-value", os);
    JsonSchema fetched = (JsonSchema) client().getSchemaById(id);
    check("schema by id carries its reference", fetched.references().size() == 1);
    boolean good;
    try {
      fetched.validate(JSON.readTree("{\"c\":{\"id\":5}}"));
      good = true;
    } catch (Exception e) {
      good = false;
    }
    check("referenced definition validates good data", good);
    boolean rejected;
    try {
      fetched.validate(JSON.readTree("{\"c\":{\"id\":\"x\"}}"));
      rejected = false;
    } catch (Exception e) {
      rejected = true;
    }
    check("referenced definition rejects bad data", rejected);
  }

  // ------------------------------------------------------------------ contexts & deletes

  static void contexts(CachedSchemaRegistryClient sr) throws Exception {
    section("contexts");
    String ctx = ".jdev" + RUN;
    AvroSchema devOnly = new AvroSchema("{\"type\":\"record\",\"name\":\"OnlyDev\",\"fields\":[{\"name\":\"z\",\"type\":\"long\"}]}");
    // Contexts have separate id spaces: push this context's counter past the
    // default context's ids, so the id below exists only here.
    int maxDefault = sr.getAllSubjects().size() + 50;
    for (int i = 0; i < maxDefault; i++) {
      sr.register(":" + ctx + ":filler-" + i, new AvroSchema("{\"type\":\"fixed\",\"name\":\"F" + i + "\",\"size\":" + (i + 1) + "}"));
    }
    String topic = n("thing");
    int devId = sr.register(":" + ctx + ":" + topic + "-value", devOnly);
    check("getAllContexts", sr.getAllContexts().contains(ctx));
    GenericRecord r2 = new GenericData.Record(devOnly.rawSchema());
    r2.put("z", 42L);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(devId).array());
    out.write(avroBody(devOnly.rawSchema(), r2));
    GenericRecord got = (GenericRecord) new KafkaAvroDeserializer(client(), conf).deserialize(topic, out.toByteArray());
    check("deserializer resolves an id via the same-named subject in another context", ((Long) got.get("z")) == 42L);
    try {
      new KafkaAvroDeserializer(client(), conf).deserialize(n("unrelated-topic"), out.toByteArray());
      check("unrelated topic cannot see another context's id", false);
    } catch (org.apache.kafka.common.errors.SerializationException e) {
      check("unrelated topic cannot see another context's id (40403, as Confluent)", true);
    }
  }

  static void deletes(CachedSchemaRegistryClient sr) throws Exception {
    section("deletes");
    String subject = n("jdel") + "-value";
    sr.register(subject, new JsonSchema("{\"type\":\"string\"}"));
    check("deleteSubject (soft)", sr.deleteSubject(subject).equals(List.of(1)));
    check("deleteSubject (permanent)", sr.deleteSubject(subject, true).equals(List.of(1)));
    try {
      client().getLatestSchemaMetadata(subject);
      check("deleted subject is gone", false);
    } catch (RestClientException e) {
      check("deleted subject is gone (40401)", e.getErrorCode() == 40401);
    }
  }
}
