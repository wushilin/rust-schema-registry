//! Render a `FileDescriptorProto` back into `.proto` source.
//!
//! Non-Java Confluent clients (Python, Go, .NET) register Protobuf schemas as
//! base64-encoded `FileDescriptorProto`s. Confluent stores and returns them as
//! text, so we do the same. Custom options (extensions) are not rendered; the
//! caller verifies the output compiles and otherwise keeps the base64 form.

use std::fmt::Write;

use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto};

pub fn render(fd: &FileDescriptorProto) -> String {
    let mut out = String::new();
    let syntax = fd.syntax.as_deref().filter(|s| !s.is_empty()).unwrap_or("proto2");
    let proto3 = syntax == "proto3";
    let _ = writeln!(out, "syntax = \"{syntax}\";");
    if let Some(p) = fd.package.as_deref().filter(|p| !p.is_empty()) {
        let _ = writeln!(out, "package {p};");
    }
    if !fd.dependency.is_empty() {
        out.push('\n');
        for (i, d) in fd.dependency.iter().enumerate() {
            let kw = if fd.public_dependency.contains(&(i as i32)) {
                "import public"
            } else if fd.weak_dependency.contains(&(i as i32)) {
                "import weak"
            } else {
                "import"
            };
            let _ = writeln!(out, "{kw} \"{}\";", escape(d));
        }
    }
    if let Some(o) = &fd.options {
        let mut opts: Vec<String> = Vec::new();
        let s = |k: &str, v: &Option<String>| v.as_ref().map(|v| format!("option {k} = \"{}\";", escape(v)));
        let b = |k: &str, v: Option<bool>| v.map(|v| format!("option {k} = {v};"));
        opts.extend(s("java_package", &o.java_package));
        opts.extend(s("java_outer_classname", &o.java_outer_classname));
        opts.extend(b("java_multiple_files", o.java_multiple_files));
        opts.extend(s("go_package", &o.go_package));
        opts.extend(s("csharp_namespace", &o.csharp_namespace));
        opts.extend(s("objc_class_prefix", &o.objc_class_prefix));
        opts.extend(s("php_namespace", &o.php_namespace));
        opts.extend(s("ruby_package", &o.ruby_package));
        opts.extend(s("swift_prefix", &o.swift_prefix));
        opts.extend(b("cc_enable_arenas", o.cc_enable_arenas));
        opts.extend(b("deprecated", o.deprecated));
        if let Some(v) = o.optimize_for {
            let name = match v {
                2 => "CODE_SIZE",
                3 => "LITE_RUNTIME",
                _ => "SPEED",
            };
            opts.push(format!("option optimize_for = {name};"));
        }
        if !opts.is_empty() {
            out.push('\n');
            for o in opts {
                let _ = writeln!(out, "{o}");
            }
        }
    }
    for m in &fd.message_type {
        out.push('\n');
        message(&mut out, m, 0, proto3);
    }
    for e in &fd.enum_type {
        out.push('\n');
        enumeration(&mut out, e, 0);
    }
    extensions(&mut out, &fd.extension, 0, proto3);
    for s in &fd.service {
        out.push('\n');
        let _ = writeln!(out, "service {} {{", s.name());
        for m in &s.method {
            let cs = if m.client_streaming() { "stream " } else { "" };
            let ss = if m.server_streaming() { "stream " } else { "" };
            let _ = writeln!(out, "  rpc {}({cs}{}) returns ({ss}{});", m.name(), qualified(m.input_type()), qualified(m.output_type()));
        }
        out.push_str("}\n");
    }
    out
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn indent(n: usize) -> String {
    "  ".repeat(n)
}

fn scalar_name(t: Type) -> Option<&'static str> {
    Some(match t {
        Type::Double => "double",
        Type::Float => "float",
        Type::Int64 => "int64",
        Type::Uint64 => "uint64",
        Type::Int32 => "int32",
        Type::Fixed64 => "fixed64",
        Type::Fixed32 => "fixed32",
        Type::Bool => "bool",
        Type::String => "string",
        Type::Bytes => "bytes",
        Type::Uint32 => "uint32",
        Type::Sfixed32 => "sfixed32",
        Type::Sfixed64 => "sfixed64",
        Type::Sint32 => "sint32",
        Type::Sint64 => "sint64",
        Type::Group | Type::Message | Type::Enum => return None,
    })
}

/// Descriptors use `.pkg.Type`; print `pkg.Type` like Confluent does, since
/// Java's (Wire) parser rejects the leading dot.
fn qualified(name: &str) -> &str {
    name.strip_prefix('.').unwrap_or(name)
}

fn type_name(f: &FieldDescriptorProto) -> String {
    // Confluent's descriptors leave `type` unset for names it couldn't resolve.
    if f.r#type.is_none() && f.type_name.is_some() {
        return qualified(f.type_name()).to_string();
    }
    scalar_name(f.r#type()).map(String::from).unwrap_or_else(|| qualified(f.type_name()).to_string())
}

fn default_json_name(name: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for c in name.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn field_options(f: &FieldDescriptorProto, proto3: bool) -> String {
    let mut opts = Vec::new();
    if !proto3 && let Some(d) = &f.default_value {
        let v = match f.r#type() {
            Type::String | Type::Bytes => format!("\"{}\"", escape(d)),
            _ => d.clone(),
        };
        opts.push(format!("default = {v}"));
    }
    if let Some(j) = f.json_name.as_deref()
        && j != default_json_name(f.name())
    {
        opts.push(format!("json_name = \"{}\"", escape(j)));
    }
    if let Some(o) = &f.options {
        if let Some(p) = o.packed {
            opts.push(format!("packed = {p}"));
        }
        if o.deprecated == Some(true) {
            opts.push("deprecated = true".into());
        }
    }
    if opts.is_empty() { String::new() } else { format!(" [{}]", opts.join(", ")) }
}

/// Find a nested map-entry type by the field's type name.
fn map_entry<'a>(m: &'a DescriptorProto, f: &FieldDescriptorProto) -> Option<&'a DescriptorProto> {
    let message_like = f.r#type() == Type::Message || (f.r#type.is_none() && f.type_name.is_some());
    if !message_like || f.label() != Label::Repeated {
        return None;
    }
    let short = f.type_name().rsplit('.').next()?;
    m.nested_type
        .iter()
        .find(|n| n.name() == short && n.options.as_ref().and_then(|o| o.map_entry) == Some(true))
}

fn field_line(out: &mut String, m: &DescriptorProto, f: &FieldDescriptorProto, depth: usize, proto3: bool, in_oneof: bool) {
    let pad = indent(depth);
    if let Some(entry) = map_entry(m, f) {
        let k = entry.field.iter().find(|x| x.number() == 1).map(type_name).unwrap_or_default();
        let v = entry.field.iter().find(|x| x.number() == 2).map(type_name).unwrap_or_default();
        let _ = writeln!(out, "{pad}map<{k}, {v}> {} = {}{};", f.name(), f.number(), field_options(f, proto3));
        return;
    }
    let label = if in_oneof {
        ""
    } else {
        match f.label() {
            Label::Repeated => "repeated ",
            Label::Required => "required ",
            Label::Optional if proto3 && f.proto3_optional() => "optional ",
            Label::Optional if !proto3 => "optional ",
            Label::Optional => "",
        }
    };
    let _ = writeln!(out, "{pad}{label}{} {} = {}{};", type_name(f), f.name(), f.number(), field_options(f, proto3));
}

fn reserved_ranges(out: &mut String, pad: &str, ranges: &[(i32, i32)], names: &[String], max: i32) {
    if !ranges.is_empty() {
        let parts: Vec<String> = ranges
            .iter()
            .map(|(s, e)| match *e {
                e if e >= max => format!("{s} to max"),
                e if e - 1 == *s => s.to_string(),
                e => format!("{s} to {}", e - 1),
            })
            .collect();
        let _ = writeln!(out, "{pad}reserved {};", parts.join(", "));
    }
    if !names.is_empty() {
        let parts: Vec<String> = names.iter().map(|n| format!("\"{}\"", escape(n))).collect();
        let _ = writeln!(out, "{pad}reserved {};", parts.join(", "));
    }
}

fn message(out: &mut String, m: &DescriptorProto, depth: usize, proto3: bool) {
    let pad = indent(depth);
    let _ = writeln!(out, "{pad}message {} {{", m.name());
    if m.options.as_ref().and_then(|o| o.deprecated) == Some(true) {
        let _ = writeln!(out, "{}option deprecated = true;", indent(depth + 1));
    }
    // Oneofs that only exist for proto3 `optional` are synthetic.
    let real_oneof = |idx: i32| m.field.iter().any(|f| f.oneof_index == Some(idx) && !f.proto3_optional());
    let mut printed_oneofs = Vec::new();
    for f in &m.field {
        match f.oneof_index {
            Some(idx) if !f.proto3_optional() && real_oneof(idx) => {
                if printed_oneofs.contains(&idx) {
                    continue;
                }
                printed_oneofs.push(idx);
                let name = m.oneof_decl.get(idx as usize).map(|o| o.name()).unwrap_or("oneof");
                let _ = writeln!(out, "{}oneof {name} {{", indent(depth + 1));
                for of in m.field.iter().filter(|x| x.oneof_index == Some(idx)) {
                    field_line(out, m, of, depth + 2, proto3, true);
                }
                let _ = writeln!(out, "{}}}", indent(depth + 1));
            }
            _ => field_line(out, m, f, depth + 1, proto3, false),
        }
    }
    for n in &m.nested_type {
        if n.options.as_ref().and_then(|o| o.map_entry) == Some(true) {
            continue;
        }
        message(out, n, depth + 1, proto3);
    }
    for e in &m.enum_type {
        enumeration(out, e, depth + 1);
    }
    for r in &m.extension_range {
        let end = r.end();
        let range = if end >= 536_870_912 { format!("{} to max", r.start()) } else { format!("{} to {}", r.start(), end - 1) };
        let _ = writeln!(out, "{}extensions {range};", indent(depth + 1));
    }
    let ranges: Vec<(i32, i32)> = m.reserved_range.iter().map(|r| (r.start(), r.end())).collect();
    reserved_ranges(out, &indent(depth + 1), &ranges, &m.reserved_name, 536_870_912);
    extensions(out, &m.extension, depth + 1, proto3);
    let _ = writeln!(out, "{pad}}}");
}

fn enumeration(out: &mut String, e: &EnumDescriptorProto, depth: usize) {
    let pad = indent(depth);
    let _ = writeln!(out, "{pad}enum {} {{", e.name());
    if e.options.as_ref().and_then(|o| o.allow_alias) == Some(true) {
        let _ = writeln!(out, "{}option allow_alias = true;", indent(depth + 1));
    }
    for v in &e.value {
        let _ = writeln!(out, "{}{} = {};", indent(depth + 1), v.name(), v.number());
    }
    // Enum reserved ranges are inclusive.
    let ranges: Vec<(i32, i32)> = e.reserved_range.iter().map(|r| (r.start(), r.end().saturating_add(1))).collect();
    reserved_ranges(out, &indent(depth + 1), &ranges, &e.reserved_name, i32::MAX);
    let _ = writeln!(out, "{pad}}}");
}

fn extensions(out: &mut String, exts: &[FieldDescriptorProto], depth: usize, proto3: bool) {
    let mut extendees: Vec<&str> = Vec::new();
    for x in exts {
        if !extendees.contains(&x.extendee()) {
            extendees.push(x.extendee());
        }
    }
    let empty = DescriptorProto::default();
    for ext in extendees {
        let _ = writeln!(out, "{}extend {} {{", indent(depth), qualified(ext));
        for f in exts.iter().filter(|f| f.extendee() == ext) {
            field_line(out, &empty, f, depth + 1, proto3, false);
        }
        let _ = writeln!(out, "{}}}", indent(depth));
    }
}
