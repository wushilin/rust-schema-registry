#!/usr/bin/env python3
"""Drive the server with Confluent's official Python client (confluent-kafka).

Usage: pip install "confluent-kafka[avro,schemaregistry,json,protobuf]"
       python3 tests/confluent_client.py http://127.0.0.1:8081 [user:password]
"""

import sys

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaReference
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

url = sys.argv[1]
conf = {"url": url}
if len(sys.argv) > 2:
    conf["basic.auth.user.info"] = sys.argv[2]
sr = SchemaRegistryClient(conf)
FAILED = []


def check(label, cond):
    print(("  ok   " if cond else "  FAIL ") + label)
    if not cond:
        FAILED.append(label)


ctx = SerializationContext("users", MessageField.VALUE)

print("avro serde")
user_v1 = '{"type":"record","name":"User","namespace":"t","fields":[{"name":"name","type":"string"}]}'
ser = AvroSerializer(sr, user_v1)
payload = ser({"name": "alice"}, ctx)
check("serializer auto-registered and framed payload", payload[0] == 0 and len(payload) > 5)
schema_id = int.from_bytes(payload[1:5], "big")
de = AvroDeserializer(SchemaRegistryClient(conf))
check("deserializer fetched schema by id", de(payload, ctx) == {"name": "alice"})
latest = sr.get_latest_version("users-value")
check("get_latest_version", latest.schema_id == schema_id and latest.version == 1)
check("get_version", sr.get_version("users-value", 1).schema.schema_str == latest.schema.schema_str)
check("get_subjects", "users-value" in sr.get_subjects())
check("get_versions", sr.get_versions("users-value") == [1])
reg = sr.lookup_schema("users-value", Schema(user_v1, "AVRO"))
check("lookup_schema", reg.version == 1 and reg.schema_id == schema_id)

print("compatibility & config")
bad = Schema('{"type":"record","name":"User","namespace":"t","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', "AVRO")
check("test_compatibility false", sr.test_compatibility("users-value", bad) is False)
try:
    sr.register_schema("users-value", bad)
    check("incompatible registration rejected", False)
except SchemaRegistryError as e:
    check("incompatible registration rejected with 409", e.http_status_code == 409 and e.error_code == 409)
sr.set_compatibility("users-value", "NONE")
check("get_compatibility", sr.get_compatibility("users-value") == "NONE")
check("register after NONE", sr.register_schema("users-value", bad) > schema_id)
check("global compatibility", sr.get_compatibility() == "BACKWARD")

print("references")
addr = Schema('{"type":"record","name":"Addr","namespace":"t","fields":[{"name":"street","type":"string"}]}', "AVRO")
sr.register_schema("addr", addr)
person = Schema('{"type":"record","name":"P","namespace":"t","fields":[{"name":"a","type":"t.Addr"}]}', "AVRO",
                [SchemaReference("t.Addr", "addr", 1)])
pid = sr.register_schema("person-value", person)
fetched = sr.get_schema(pid)
check("references round-trip", fetched.references[0].subject == "addr" and fetched.references[0].version == 1)
pser = AvroSerializer(sr, fetched, conf={"auto.register.schemas": False, "use.latest.version": True})
pctx = SerializationContext("person", MessageField.VALUE)
pdata = pser({"a": {"street": "main"}}, pctx)
check("serialize with referenced schema", AvroDeserializer(sr)(pdata, pctx) == {"a": {"street": "main"}})

print("json schema serde")
js = '{"$schema":"http://json-schema.org/draft-07/schema#","title":"Item","type":"object","properties":{"sku":{"type":"string"}},"required":["sku"]}'
jctx = SerializationContext("items", MessageField.VALUE)
jser = JSONSerializer(js, sr)
jdata = jser({"sku": "x1"}, jctx)
check("json serializer", jdata[0] == 0)
check("json deserializer", JSONDeserializer(None, schema_registry_client=sr)(jdata, jctx) == {"sku": "x1"})

print("protobuf serde (client sends base64 FileDescriptorProto)")
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, timestamp_pb2
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

pool = descriptor_pool.DescriptorPool()
pool.AddSerializedFile(timestamp_pb2.DESCRIPTOR.serialized_pb)
common = descriptor_pb2.FileDescriptorProto(name="acme/common.proto", package="acme", syntax="proto3")
money = common.message_type.add(name="Money")
money.field.add(name="cents", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
                label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
pool.Add(common)
order_fd = descriptor_pb2.FileDescriptorProto(name="acme/order.proto", package="acme", syntax="proto3",
                                              dependency=["acme/common.proto", "google/protobuf/timestamp.proto"])
order = order_fd.message_type.add(name="Order")
F = descriptor_pb2.FieldDescriptorProto
order.field.add(name="id", number=1, type=F.TYPE_STRING, label=F.LABEL_OPTIONAL)
order.field.add(name="total", number=2, type=F.TYPE_MESSAGE, type_name=".acme.Money", label=F.LABEL_OPTIONAL)
order.field.add(name="at", number=3, type=F.TYPE_MESSAGE, type_name=".google.protobuf.Timestamp", label=F.LABEL_OPTIONAL)
order.field.add(name="tags", number=4, type=F.TYPE_STRING, label=F.LABEL_REPEATED)
pool.Add(order_fd)
Order = message_factory.GetMessageClass(pool.FindMessageTypeByName("acme.Order"))
Money = message_factory.GetMessageClass(pool.FindMessageTypeByName("acme.Money"))
octx = SerializationContext("orders", MessageField.VALUE)
pser = ProtobufSerializer(Order, sr, {"use.deprecated.format": False})
msg = Order(id="o-1", total=Money(cents=1234), tags=["a", "b"])
msg.at.seconds = 1700000000
odata = pser(msg, octx)
check("protobuf serializer registered schema + reference", odata[0] == 0)
check("dependency registered as its own subject", "acme/common.proto" in sr.get_subjects())
stored = sr.get_latest_version("orders-value")
check("stored as .proto text (like Confluent)", "message Order" in stored.schema.schema_str and stored.schema.schema_type == "PROTOBUF")
check("reference recorded", stored.schema.references[0].subject == "acme/common.proto")
pde = ProtobufDeserializer(Order, {"use.deprecated.format": False}, schema_registry_client=SchemaRegistryClient(conf))
back = pde(odata, octx)
check("protobuf round trip", back.id == "o-1" and back.total.cents == 1234 and list(back.tags) == ["a", "b"])
check("re-serialize is idempotent (same id)", pser(msg, octx)[1:5] == odata[1:5])

print("avro evolution: v1 data through a v2 reader and back")
ev1 = '{"type":"record","name":"Ev","namespace":"py","fields":[{"name":"id","type":"long"}]}'
ev2 = '{"type":"record","name":"Ev","namespace":"py","fields":[{"name":"id","type":"long"},{"name":"name","type":"string","default":"n/a"}]}'
ectx = SerializationContext("pyevo", MessageField.VALUE)
b1 = AvroSerializer(sr, ev1)({"id": 7}, ectx)
id2 = sr.register_schema("pyevo-value", Schema(ev2, "AVRO"))
check("v2 registered", id2 != int.from_bytes(b1[1:5], "big"))
check("v1 bytes read with v2 reader schema: default applied",
      AvroDeserializer(SchemaRegistryClient(conf), ev2)(b1, ectx) == {"id": 7, "name": "n/a"})
b2 = AvroSerializer(sr, ev2, conf={"auto.register.schemas": False, "use.latest.version": True})({"id": 8, "name": "eve"}, ectx)
check("use.latest.version writes with v2's id", int.from_bytes(b2[1:5], "big") == id2)
check("v2 bytes read with v1 reader schema: new field dropped",
      AvroDeserializer(SchemaRegistryClient(conf), ev1)(b2, ectx) == {"id": 8})

print("protobuf evolution: v1 data through a v2 message type and back")
def proto_class(fields, fname):
    pool2 = descriptor_pool.DescriptorPool()
    fd = descriptor_pb2.FileDescriptorProto(name=fname, package="pyevo", syntax="proto3")
    m = fd.message_type.add(name="Evo")
    for name, number, ftype in fields:
        m.field.add(name=name, number=number, type=ftype, label=F.LABEL_OPTIONAL)
    pool2.Add(fd)
    return message_factory.GetMessageClass(pool2.FindMessageTypeByName("pyevo.Evo"))
EvoV1 = proto_class([("id", 1, F.TYPE_STRING)], "pyevo/evo.proto")
EvoV2 = proto_class([("id", 1, F.TYPE_STRING), ("qty", 2, F.TYPE_INT32)], "pyevo/evo.proto")
pctx2 = SerializationContext("pyprotoevo", MessageField.VALUE)
pb1 = ProtobufSerializer(EvoV1, sr, {"use.deprecated.format": False})(EvoV1(id="a"), pctx2)
pb2 = ProtobufSerializer(EvoV2, sr, {"use.deprecated.format": False})(EvoV2(id="b", qty=3), pctx2)
check("v2 auto-registered as version 2", sr.get_versions("pyprotoevo-value") == [1, 2])
as_v2 = ProtobufDeserializer(EvoV2, {"use.deprecated.format": False})(pb1, pctx2)
check("v1 bytes decoded as v2: new field defaults to 0", as_v2.id == "a" and as_v2.qty == 0)
as_v1 = ProtobufDeserializer(EvoV1, {"use.deprecated.format": False})(pb2, pctx2)
check("v2 bytes decoded as v1: known field intact", as_v1.id == "b")

print("json schema evolution: closed model, optional property added")
jv1 = '{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}'
jv2 = '{"type":"object","properties":{"sku":{"type":"string"},"qty":{"type":"integer"}},"required":["sku"],"additionalProperties":false}'
jctx2 = SerializationContext("pyjsonevo", MessageField.VALUE)
jb1 = JSONSerializer(jv1, sr)({"sku": "a"}, jctx2)
check("v2 is backward compatible", sr.test_compatibility("pyjsonevo-value", Schema(jv2, "JSON")))
JSONSerializer(jv2, sr)({"sku": "b", "qty": 2}, jctx2)
check("v1 bytes validate and decode against the v2 reader schema",
      JSONDeserializer(jv2, schema_registry_client=SchemaRegistryClient(conf))(jb1, jctx2) == {"sku": "a"})
try:
    JSONSerializer(jv1, sr)({"sku": "a", "qty": 1}, jctx2)
    check("v1 serializer rejects data v1 doesn't allow", False)
except Exception:
    check("v1 serializer rejects data v1 doesn't allow", True)

print("deletes")
check("delete_version", sr.delete_version("users-value", 2) == 2)
check("delete_subject", sr.delete_subject("users-value") == [1])
check("delete_subject permanent", sr.delete_subject("users-value", permanent=True) == [1, 2])
try:
    SchemaRegistryClient(conf).get_latest_version("users-value")  # fresh client: no local cache
    check("deleted subject gone", False)
except SchemaRegistryError as e:
    check("deleted subject gone (40401)", e.error_code == 40401)

print()
if FAILED:
    print(f"{len(FAILED)} FAILED")
    sys.exit(1)
print("confluent client checks passed")
