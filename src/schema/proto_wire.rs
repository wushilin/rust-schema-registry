//! Source-level `.proto` model and printer matching Square Wire's
//! `ProtoFileElement.toSchema()`, which is how Confluent canonicalizes the
//! Protobuf schemas it stores (with comments dropped and no header).
//!
//! Unlike a descriptor, this keeps the schema as written: type names are not
//! resolved, options keep their source order and values, reserved statements
//! keep their grouping. The input has already been validated by `protox`, so
//! the parser only needs to handle well-formed files; anything it doesn't
//! model (e.g. proto2 groups) makes it return `None` and the caller falls back.

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String), // raw contents between quotes, escapes preserved
    Num(String),
    Sym(char),
}

fn tokenize(src: &str) -> Option<Vec<Tok>> {
    let c: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '/' && c.get(i + 1) == Some(&'/') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && c.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < c.len() && !(c[i] == '*' && c[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if ch == '"' || ch == '\'' {
            let q = ch;
            i += 1;
            let mut s = String::new();
            while i < c.len() && c[i] != q {
                if c[i] == '\\' && i + 1 < c.len() {
                    s.push(c[i]);
                    i += 1;
                }
                s.push(c[i]);
                i += 1;
            }
            i += 1;
            // Adjacent string literals concatenate.
            if let Some(Tok::Str(prev)) = out.last_mut() {
                prev.push_str(&s);
            } else {
                out.push(Tok::Str(s));
            }
        } else if ch.is_ascii_digit() || (ch == '-' && c.get(i + 1).is_some_and(|d| d.is_ascii_digit() || *d == '.')) {
            let start = i;
            i += 1;
            while i < c.len() && (c[i].is_ascii_alphanumeric() || c[i] == '.' || ((c[i] == '-' || c[i] == '+') && matches!(c[i - 1], 'e' | 'E'))) {
                i += 1;
            }
            out.push(Tok::Num(c[start..i].iter().collect()));
        } else if ch.is_alphabetic() || ch == '_' || (ch == '.' && c.get(i + 1).is_some_and(|d| d.is_alphabetic() || *d == '_')) {
            let start = i;
            i += 1;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '.') {
                i += 1;
            }
            out.push(Tok::Ident(c[start..i].iter().collect()));
        } else if "{}[]()<>;,=:-".contains(ch) {
            out.push(Tok::Sym(ch));
            i += 1;
        } else {
            return None;
        }
    }
    Some(out)
}

#[derive(Debug, Clone)]
enum Value {
    Str(String),
    Raw(String),
    Map(Vec<(String, Value)>),
    List(Vec<Value>),
}

#[derive(Debug, Clone)]
struct Opt {
    name: String,
    value: Value,
}

#[derive(Debug, Default)]
struct Field {
    /// Set by normalization: options are already in their final (sorted) order.
    sorted: bool,
    label: Option<String>,
    ty: String,
    name: String,
    tag: String,
    options: Vec<Opt>,
}

#[derive(Debug, Default)]
struct OneOf {
    name: String,
    options: Vec<Opt>,
    fields: Vec<Field>,
}

#[derive(Debug)]
enum TypeEl {
    Message(Message),
    Enum(EnumEl),
}

#[derive(Debug, Default)]
struct Message {
    name: String,
    reserved: Vec<String>,
    options: Vec<Opt>,
    fields: Vec<Field>,
    oneofs: Vec<OneOf>,
    extensions: Vec<String>,
    nested: Vec<TypeEl>,
    extends: Vec<Extend>,
}

#[derive(Debug, Default)]
struct EnumEl {
    name: String,
    reserved: Vec<String>,
    options: Vec<Opt>,
    constants: Vec<(String, String, Vec<Opt>)>,
}

#[derive(Debug, Default)]
struct Extend {
    name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Default)]
struct Rpc {
    name: String,
    req: String,
    req_stream: bool,
    resp: String,
    resp_stream: bool,
    options: Vec<Opt>,
}

#[derive(Debug, Default)]
struct Service {
    name: String,
    options: Vec<Opt>,
    rpcs: Vec<Rpc>,
}

#[derive(Debug, Default)]
struct File {
    syntax: Option<String>,
    package: Option<String>,
    imports: Vec<String>,
    public_imports: Vec<String>,
    options: Vec<Opt>,
    types: Vec<TypeEl>,
    extends: Vec<Extend>,
    services: Vec<Service>,
}

struct Parser {
    t: Vec<Tok>,
    i: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        self.i += 1;
        t
    }
    fn sym(&mut self, c: char) -> Option<()> {
        (self.next()? == Tok::Sym(c)).then_some(())
    }
    fn is_sym(&self, c: char) -> bool {
        self.peek() == Some(&Tok::Sym(c))
    }
    fn ident(&mut self) -> Option<String> {
        match self.next()? {
            Tok::Ident(s) => Some(s),
            _ => None,
        }
    }
    fn is_ident(&self, s: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(x)) if x == s)
    }
    fn string(&mut self) -> Option<String> {
        match self.next()? {
            Tok::Str(s) => Some(s),
            _ => None,
        }
    }
    fn skip_semis(&mut self) {
        while self.is_sym(';') {
            self.i += 1;
        }
    }

    fn file(&mut self) -> Option<File> {
        let mut f = File::default();
        while self.peek().is_some() {
            if self.is_sym(';') {
                self.i += 1;
                continue;
            }
            match self.ident()?.as_str() {
                "syntax" | "edition" => {
                    self.sym('=')?;
                    f.syntax = Some(self.string()?);
                    self.sym(';')?;
                }
                "package" => {
                    f.package = Some(self.ident()?);
                    self.sym(';')?;
                }
                "import" => {
                    let public = self.is_ident("public");
                    if public || self.is_ident("weak") {
                        self.i += 1;
                    }
                    let path = self.string()?;
                    self.sym(';')?;
                    if public { f.public_imports.push(path) } else { f.imports.push(path) }
                }
                "option" => f.options.push(self.option_decl()?),
                "message" => f.types.push(TypeEl::Message(self.message()?)),
                "enum" => f.types.push(TypeEl::Enum(self.enumeration()?)),
                "extend" => f.extends.push(self.extend()?),
                "service" => f.services.push(self.service()?),
                _ => return None,
            }
        }
        Some(f)
    }

    fn option_name(&mut self) -> Option<String> {
        let mut name = String::new();
        loop {
            if self.is_sym('(') {
                self.i += 1;
                name.push('(');
                name.push_str(&self.ident()?);
                self.sym(')')?;
                name.push(')');
            } else {
                name.push_str(&self.ident()?);
            }
            // `(a).b` continues with an identifier starting with '.'
            match self.peek() {
                Some(Tok::Ident(s)) if s.starts_with('.') => {
                    name.push_str(s);
                    self.i += 1;
                    return Some(name);
                }
                _ => return Some(name),
            }
        }
    }

    /// `option name = value;` (after the `option` keyword).
    fn option_decl(&mut self) -> Option<Opt> {
        let name = self.option_name()?;
        self.sym('=')?;
        let value = self.value()?;
        self.sym(';')?;
        Some(Opt { name, value })
    }

    fn value(&mut self) -> Option<Value> {
        match self.next()? {
            Tok::Str(s) => Some(Value::Str(s)),
            Tok::Num(n) => Some(Value::Raw(n)),
            Tok::Ident(s) => Some(Value::Raw(s)),
            Tok::Sym('-') => match self.next()? {
                Tok::Ident(s) | Tok::Num(s) => Some(Value::Raw(format!("-{s}"))),
                _ => None,
            },
            Tok::Sym('{') => self.aggregate(),
            Tok::Sym('[') => {
                let mut items = Vec::new();
                while !self.is_sym(']') {
                    items.push(self.value()?);
                    if self.is_sym(',') {
                        self.i += 1;
                    }
                }
                self.i += 1;
                Some(Value::List(items))
            }
            _ => None,
        }
    }

    /// Text-format message literal (after `{`). Repeated keys collapse into a list, as in Wire.
    fn aggregate(&mut self) -> Option<Value> {
        let mut entries: Vec<(String, Value)> = Vec::new();
        while !self.is_sym('}') {
            let key = if self.is_sym('[') {
                self.i += 1;
                let k = format!("[{}]", self.ident()?);
                self.sym(']')?;
                k
            } else {
                self.ident()?
            };
            if self.is_sym(':') {
                self.i += 1;
            }
            let v = self.value()?;
            if let Some((_, existing)) = entries.iter_mut().find(|(k, _)| *k == key) {
                match existing {
                    Value::List(l) => l.push(v),
                    other => *other = Value::List(vec![other.clone(), v]),
                }
            } else {
                entries.push((key, v));
            }
            if self.is_sym(',') || self.is_sym(';') {
                self.i += 1;
            }
        }
        self.i += 1;
        Some(Value::Map(entries))
    }

    /// `[a = 1, (b) = 2]`
    fn field_options(&mut self) -> Option<Vec<Opt>> {
        let mut opts = Vec::new();
        if !self.is_sym('[') {
            return Some(opts);
        }
        self.i += 1;
        loop {
            let name = self.option_name()?;
            self.sym('=')?;
            let value = self.value()?;
            opts.push(Opt { name, value });
            if self.is_sym(',') {
                self.i += 1;
            } else {
                break;
            }
        }
        self.sym(']')?;
        Some(opts)
    }

    fn field(&mut self, first: String, in_oneof: bool) -> Option<Field> {
        let mut f = Field::default();
        let mut ty = first;
        if !in_oneof && matches!(ty.as_str(), "optional" | "required" | "repeated") {
            f.label = Some(ty);
            ty = self.ident()?;
        }
        if ty == "map" && self.is_sym('<') {
            self.i += 1;
            let k = self.ident()?;
            self.sym(',')?;
            let v = self.ident()?;
            self.sym('>')?;
            ty = format!("map<{k}, {v}>");
        }
        if ty == "group" {
            return None;
        }
        f.ty = ty;
        f.name = self.ident()?;
        self.sym('=')?;
        f.tag = match self.next()? {
            Tok::Num(n) => n,
            _ => return None,
        };
        f.options = self.field_options()?;
        self.sym(';')?;
        Some(f)
    }

    /// `reserved 1, 2 to 5, "a";` / `extensions 1 to max;` rendered like Wire.
    fn ranges(&mut self, kw: &str) -> Option<String> {
        let mut parts = Vec::new();
        loop {
            match self.next()? {
                Tok::Str(s) => parts.push(format!("\"{s}\"")),
                Tok::Ident(s) => parts.push(s),
                Tok::Num(a) => {
                    if self.is_ident("to") {
                        self.i += 1;
                        let b = match self.next()? {
                            Tok::Num(b) | Tok::Ident(b) => b,
                            _ => return None,
                        };
                        parts.push(format!("{a} to {b}"));
                    } else {
                        parts.push(a);
                    }
                }
                _ => return None,
            }
            if self.is_sym(',') {
                self.i += 1;
            } else {
                break;
            }
        }
        // extensions may carry options; Wire keeps them but they are rare
        if self.is_sym('[') {
            self.field_options()?;
        }
        self.sym(';')?;
        Some(format!("{kw} {};", parts.join(", ")))
    }

    fn message(&mut self) -> Option<Message> {
        let mut m = Message { name: self.ident()?, ..Default::default() };
        self.sym('{')?;
        loop {
            self.skip_semis();
            if self.is_sym('}') {
                self.i += 1;
                break;
            }
            let kw = self.ident()?;
            match kw.as_str() {
                "option" => m.options.push(self.option_decl()?),
                "reserved" => m.reserved.push(self.ranges("reserved")?),
                "extensions" => m.extensions.push(self.ranges("extensions")?),
                "message" => m.nested.push(TypeEl::Message(self.message()?)),
                "enum" => m.nested.push(TypeEl::Enum(self.enumeration()?)),
                "extend" => m.extends.push(self.extend()?),
                "oneof" => m.oneofs.push(self.oneof()?),
                _ => m.fields.push(self.field(kw, false)?),
            }
        }
        Some(m)
    }

    fn oneof(&mut self) -> Option<OneOf> {
        let mut o = OneOf { name: self.ident()?, ..Default::default() };
        self.sym('{')?;
        loop {
            self.skip_semis();
            if self.is_sym('}') {
                self.i += 1;
                break;
            }
            let kw = self.ident()?;
            if kw == "option" {
                o.options.push(self.option_decl()?);
            } else {
                o.fields.push(self.field(kw, true)?);
            }
        }
        Some(o)
    }

    fn enumeration(&mut self) -> Option<EnumEl> {
        let mut e = EnumEl { name: self.ident()?, ..Default::default() };
        self.sym('{')?;
        loop {
            self.skip_semis();
            if self.is_sym('}') {
                self.i += 1;
                break;
            }
            let kw = self.ident()?;
            match kw.as_str() {
                "option" => e.options.push(self.option_decl()?),
                "reserved" => e.reserved.push(self.ranges("reserved")?),
                name => {
                    self.sym('=')?;
                    let tag = match self.next()? {
                        Tok::Num(n) => n,
                        Tok::Sym('-') => match self.next()? {
                            Tok::Num(n) => format!("-{n}"),
                            _ => return None,
                        },
                        _ => return None,
                    };
                    let opts = self.field_options()?;
                    self.sym(';')?;
                    e.constants.push((name.to_string(), tag, opts));
                }
            }
        }
        Some(e)
    }

    fn extend(&mut self) -> Option<Extend> {
        let mut x = Extend { name: self.ident()?, ..Default::default() };
        self.sym('{')?;
        loop {
            self.skip_semis();
            if self.is_sym('}') {
                self.i += 1;
                break;
            }
            let kw = self.ident()?;
            x.fields.push(self.field(kw, false)?);
        }
        Some(x)
    }

    fn service(&mut self) -> Option<Service> {
        let mut s = Service { name: self.ident()?, ..Default::default() };
        self.sym('{')?;
        loop {
            self.skip_semis();
            if self.is_sym('}') {
                self.i += 1;
                break;
            }
            match self.ident()?.as_str() {
                "option" => s.options.push(self.option_decl()?),
                "rpc" => {
                    let mut r = Rpc { name: self.ident()?, ..Default::default() };
                    self.sym('(')?;
                    if self.is_ident("stream") {
                        self.i += 1;
                        r.req_stream = true;
                    }
                    r.req = self.ident()?;
                    self.sym(')')?;
                    if self.ident()? != "returns" {
                        return None;
                    }
                    self.sym('(')?;
                    if self.is_ident("stream") {
                        self.i += 1;
                        r.resp_stream = true;
                    }
                    r.resp = self.ident()?;
                    self.sym(')')?;
                    if self.is_sym('{') {
                        self.i += 1;
                        loop {
                            self.skip_semis();
                            if self.is_sym('}') {
                                self.i += 1;
                                break;
                            }
                            if self.ident()? != "option" {
                                return None;
                            }
                            r.options.push(self.option_decl()?);
                        }
                    } else {
                        self.sym(';')?;
                    }
                    s.rpcs.push(r);
                }
                _ => return None,
            }
        }
        Some(s)
    }
}

// ---------------- printing (Wire's toSchema) ----------------

fn append_indented(out: &mut String, value: &str) {
    let mut lines: Vec<&str> = value.split('\n').collect();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    for l in lines {
        out.push_str("  ");
        out.push_str(l);
        out.push('\n');
    }
}

fn value_str(v: &Value) -> String {
    match v {
        Value::Str(s) => format!("\"{s}\""),
        Value::Raw(r) => r.clone(),
        Value::Map(entries) => {
            let mut out = String::from("{\n");
            for (i, (k, v)) in entries.iter().enumerate() {
                let endl = if i + 1 < entries.len() { "," } else { "" };
                append_indented(&mut out, &format!("{k}: {}{endl}", value_str(v)));
            }
            out.push('}');
            out
        }
        Value::List(items) => {
            let mut out = String::from("[\n");
            for (i, v) in items.iter().enumerate() {
                let endl = if i + 1 < items.len() { "," } else { "" };
                append_indented(&mut out, &format!("{}{endl}", value_str(v)));
            }
            out.push(']');
            out
        }
    }
}

fn opt_str(o: &Opt) -> String {
    format!("{} = {}", o.name, value_str(&o.value))
}

fn opt_decl(o: &Opt) -> String {
    format!("option {};\n", opt_str(o))
}

fn append_options(out: &mut String, opts: &[Opt]) {
    if opts.len() == 1 {
        out.push('[');
        out.push_str(&opt_str(&opts[0]));
        out.push(']');
        return;
    }
    out.push_str("[\n");
    for (i, o) in opts.iter().enumerate() {
        let endl = if i + 1 < opts.len() { "," } else { "" };
        append_indented(out, &format!("{}{endl}", opt_str(o)));
    }
    out.push(']');
}

fn field_str(f: &Field) -> String {
    let mut out = String::new();
    if let Some(l) = &f.label {
        out.push_str(l);
        out.push(' ');
    }
    out.push_str(&format!("{} {} = {}", f.ty, f.name, f.tag));
    // Wire keeps `default` and `json_name` as separate attributes and prints
    // them after the other options.
    let opts: Vec<Opt> = if f.sorted {
        f.options.clone()
    } else {
        let mut o: Vec<Opt> = f.options.iter().filter(|o| o.name != "default" && o.name != "json_name").cloned().collect();
        o.extend(f.options.iter().filter(|o| o.name == "default").cloned());
        o.extend(f.options.iter().filter(|o| o.name == "json_name").cloned());
        o
    };
    if !opts.is_empty() {
        out.push(' ');
        append_options(&mut out, &opts);
    }
    out.push_str(";\n");
    out
}

/// Confluent prints messages before enums at every level (its file element is
/// rebuilt from the descriptor, where the two are separate lists).
fn ordered(types: &[TypeEl]) -> impl Iterator<Item = &TypeEl> {
    types.iter().filter(|t| matches!(t, TypeEl::Message(_))).chain(types.iter().filter(|t| matches!(t, TypeEl::Enum(_))))
}

fn type_str(t: &TypeEl) -> String {
    match t {
        TypeEl::Message(m) => message_str(m),
        TypeEl::Enum(e) => enum_str(e),
    }
}

fn message_str(m: &Message) -> String {
    let mut out = format!("message {} {{", m.name);
    if !m.reserved.is_empty() {
        out.push('\n');
        for r in &m.reserved {
            append_indented(&mut out, &format!("{r}\n"));
        }
    }
    if !m.options.is_empty() {
        out.push('\n');
        for o in &m.options {
            append_indented(&mut out, &opt_decl(o));
        }
    }
    if !m.fields.is_empty() {
        out.push('\n');
        for f in &m.fields {
            append_indented(&mut out, &field_str(f));
        }
    }
    if !m.oneofs.is_empty() {
        out.push('\n');
        for o in &m.oneofs {
            append_indented(&mut out, &oneof_str(o));
        }
    }
    if !m.extensions.is_empty() {
        out.push('\n');
        for x in &m.extensions {
            append_indented(&mut out, &format!("{x}\n"));
        }
    }
    if !m.nested.is_empty() {
        out.push('\n');
        for t in ordered(&m.nested) {
            append_indented(&mut out, &type_str(t));
        }
    }
    if !m.extends.is_empty() {
        out.push('\n');
        for x in &m.extends {
            append_indented(&mut out, &extend_str(x));
        }
    }
    out.push_str("}\n");
    out
}

fn oneof_str(o: &OneOf) -> String {
    let mut out = format!("oneof {} {{", o.name);
    if !o.options.is_empty() {
        out.push('\n');
        for x in &o.options {
            append_indented(&mut out, &opt_decl(x));
        }
    }
    if !o.fields.is_empty() {
        out.push('\n');
        for f in &o.fields {
            append_indented(&mut out, &field_str(f));
        }
    }
    out.push_str("}\n");
    out
}

fn enum_str(e: &EnumEl) -> String {
    let mut out = format!("enum {} {{", e.name);
    if !e.reserved.is_empty() {
        out.push('\n');
        for r in &e.reserved {
            append_indented(&mut out, &format!("{r}\n"));
        }
    }
    if e.reserved.is_empty() && (!e.options.is_empty() || !e.constants.is_empty()) {
        out.push('\n');
    }
    for o in &e.options {
        append_indented(&mut out, &opt_decl(o));
    }
    for (name, tag, opts) in &e.constants {
        let mut c = format!("{name} = {tag}");
        if !opts.is_empty() {
            c.push(' ');
            append_options(&mut c, opts);
        }
        c.push_str(";\n");
        append_indented(&mut out, &c);
    }
    out.push_str("}\n");
    out
}

fn extend_str(x: &Extend) -> String {
    let mut out = format!("extend {} {{", x.name);
    if !x.fields.is_empty() {
        out.push('\n');
        for f in &x.fields {
            append_indented(&mut out, &field_str(f));
        }
    }
    out.push_str("}\n");
    out
}

fn service_str(s: &Service) -> String {
    let mut out = format!("service {} {{", s.name);
    if !s.options.is_empty() {
        out.push('\n');
        for o in &s.options {
            append_indented(&mut out, &opt_decl(o));
        }
    }
    if !s.rpcs.is_empty() {
        out.push('\n');
        for r in &s.rpcs {
            let mut line = format!(
                "rpc {} ({}{}) returns ({}{})",
                r.name,
                if r.req_stream { "stream " } else { "" },
                r.req,
                if r.resp_stream { "stream " } else { "" },
                r.resp
            );
            if r.options.is_empty() {
                line.push_str(";\n");
            } else {
                line.push_str(" {\n");
                for o in &r.options {
                    append_indented(&mut line, &opt_decl(o));
                }
                line.push_str("};\n");
            }
            append_indented(&mut out, &line);
        }
    }
    out.push_str("}\n");
    out
}

fn file_str(f: &File) -> String {
    let mut out = String::new();
    if let Some(s) = &f.syntax {
        out.push_str(&format!("syntax = \"{s}\";\n"));
    }
    if let Some(p) = &f.package {
        out.push_str(&format!("package {p};\n"));
    }
    if !f.imports.is_empty() || !f.public_imports.is_empty() {
        out.push('\n');
        for i in &f.imports {
            out.push_str(&format!("import \"{i}\";\n"));
        }
        for i in &f.public_imports {
            out.push_str(&format!("import public \"{i}\";\n"));
        }
    }
    if !f.options.is_empty() {
        out.push('\n');
        for o in &f.options {
            out.push_str(&opt_decl(o));
        }
    }
    if !f.types.is_empty() {
        out.push('\n');
        for t in ordered(&f.types) {
            out.push_str(&type_str(t));
        }
    }
    if !f.extends.is_empty() {
        out.push('\n');
        for x in &f.extends {
            out.push_str(&extend_str(x));
        }
    }
    if !f.services.is_empty() {
        out.push('\n');
        for s in &f.services {
            out.push_str(&service_str(s));
        }
    }
    // Wire starts the type section with a blank line even when nothing
    // precedes it (no syntax, package, imports or options).
    out
}

// ---------------- normalization (Confluent's `ProtobufSchema#normalize`) ----------------

use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};

fn opt_sort_key(o: &Opt) -> String {
    o.name.replace(['(', ')'], "")
}

/// Sort map keys recursively and collapse single-element lists.
fn normalize_value(v: &mut Value) {
    match v {
        Value::Map(entries) => {
            for (_, x) in entries.iter_mut() {
                normalize_value(x);
                if let Value::List(items) = x
                    && items.len() == 1
                {
                    *x = items.remove(0);
                }
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
        }
        Value::List(items) => items.iter_mut().for_each(normalize_value),
        _ => {}
    }
}

fn normalize_opts(opts: &mut [Opt]) {
    for o in opts.iter_mut() {
        normalize_value(&mut o.value);
    }
    opts.sort_by_key(opt_sort_key);
}

fn scalar_or_qualified(kind: Kind) -> Option<String> {
    Some(match kind {
        Kind::Message(m) => format!(".{}", m.full_name()),
        Kind::Enum(e) => format!(".{}", e.full_name()),
        _ => return None,
    })
}

fn field_type(fd: &FieldDescriptor, written: &str) -> String {
    if fd.is_map()
        && let Kind::Message(entry) = fd.kind()
    {
        let k = entry.map_entry_key_field();
        let v = entry.map_entry_value_field();
        let kt = scalar_or_qualified(k.kind()).unwrap_or_else(|| scalar_name(&k.kind()));
        let vt = scalar_or_qualified(v.kind()).unwrap_or_else(|| scalar_name(&v.kind()));
        return format!("map<{kt}, {vt}>");
    }
    scalar_or_qualified(fd.kind()).unwrap_or_else(|| written.to_string())
}

fn scalar_name(k: &Kind) -> String {
    format!("{k:?}").to_lowercase()
}

/// Sorted reserved statements: numbers/ranges by start, then names.
fn split_reserved(stmts: &[String], kw: &str) -> Vec<String> {
    let mut nums: Vec<(i64, String)> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for s in stmts {
        let body = s.trim_start_matches(kw).trim().trim_end_matches(';');
        for part in body.split(", ") {
            let part = part.trim();
            if part.starts_with('"') {
                names.push(part.to_string());
            } else {
                let start = part.split(' ').next().and_then(|n| n.parse::<i64>().ok()).unwrap_or(0);
                nums.push((start, part.to_string()));
            }
        }
    }
    nums.sort_by_key(|(n, _)| *n);
    names.sort();
    nums.into_iter().map(|(_, p)| format!("{kw} {p};")).chain(names.into_iter().map(|n| format!("{kw} {n};"))).collect()
}

fn normalize_fields(fields: &mut Vec<Field>, md: Option<&MessageDescriptor>) {
    for f in fields.iter_mut() {
        normalize_opts(&mut f.options);
        f.sorted = true;
        if let Some(fd) = md.and_then(|m| m.get_field_by_name(&f.name)) {
            f.ty = field_type(&fd, &f.ty);
            if fd.is_map() {
                f.label = None;
            }
        }
    }
    fields.sort_by_key(|f| f.tag.parse::<i64>().unwrap_or(0));
}

fn normalize_message(m: &mut Message, full: &str, pool: &DescriptorPool) {
    let md = pool.get_message_by_name(full);
    m.reserved = split_reserved(&m.reserved, "reserved");
    normalize_opts(&mut m.options);
    normalize_fields(&mut m.fields, md.as_ref());
    for o in m.oneofs.iter_mut() {
        normalize_opts(&mut o.options);
        normalize_fields(&mut o.fields, md.as_ref());
    }
    // Explicit map-entry messages disappear (their fields became `map<>`).
    m.nested.retain(|t| !matches!(t, TypeEl::Message(n) if pool.get_message_by_name(&format!("{full}.{}", n.name)).is_some_and(|d| d.is_map_entry())));
    normalize_types(&mut m.nested, full, pool);
    for x in m.extends.iter_mut() {
        normalize_extend(x, full, pool);
    }
}

fn normalize_types(types: &mut [TypeEl], scope: &str, pool: &DescriptorPool) {
    for t in types.iter_mut() {
        match t {
            TypeEl::Message(m) => {
                let full = if scope.is_empty() { m.name.clone() } else { format!("{scope}.{}", m.name) };
                normalize_message(m, &full, pool);
            }
            TypeEl::Enum(e) => {
                e.reserved = split_reserved(&e.reserved, "reserved");
                normalize_opts(&mut e.options);
                for (_, _, o) in e.constants.iter_mut() {
                    normalize_opts(o);
                }
                e.constants.sort_by_key(|(_, n, _)| n.parse::<i64>().unwrap_or(0));
            }
        }
    }
}

fn normalize_extend(x: &mut Extend, scope: &str, pool: &DescriptorPool) {
    for f in x.fields.iter_mut() {
        normalize_opts(&mut f.options);
        f.sorted = true;
        let ext = pool.all_extensions().find(|e| {
            e.name() == f.name && (e.full_name() == format!("{scope}.{}", f.name) || e.full_name() == f.name || scope.is_empty())
        });
        if let Some(e) = ext {
            x.name = format!(".{}", e.containing_message().full_name());
            f.ty = scalar_or_qualified(e.kind()).unwrap_or_else(|| f.ty.clone());
        }
    }
    x.fields.sort_by_key(|f| f.tag.parse::<i64>().unwrap_or(0));
}

/// Confluent's normalized form of a `.proto` source: fully-qualified type
/// references, fields/constants/reserved sorted, options and imports sorted,
/// explicit map entries collapsed, and the default `proto2` syntax dropped.
pub fn normalized(src: &str, pool: &DescriptorPool) -> Option<String> {
    let tokens = tokenize(src)?;
    let mut f = Parser { t: tokens, i: 0 }.file()?;
    if f.syntax.as_deref() == Some("proto2") {
        f.syntax = None;
    }
    f.imports.sort();
    f.public_imports.sort();
    normalize_opts(&mut f.options);
    let pkg = f.package.clone().unwrap_or_default();
    normalize_types(&mut f.types, &pkg, pool);
    for x in f.extends.iter_mut() {
        normalize_extend(x, &pkg, pool);
    }
    for s in f.services.iter_mut() {
        normalize_opts(&mut s.options);
        let full = if pkg.is_empty() { s.name.clone() } else { format!("{pkg}.{}", s.name) };
        let sd = pool.get_service_by_name(&full);
        for r in s.rpcs.iter_mut() {
            normalize_opts(&mut r.options);
            if let Some(m) = sd.as_ref().and_then(|sd| sd.methods().find(|m| m.name() == r.name)) {
                r.req = format!(".{}", m.input().full_name());
                r.resp = format!(".{}", m.output().full_name());
            }
        }
    }
    Some(file_str(&f))
}

/// `format=ignore_extensions`: the canonical text without extension ranges,
/// `extend` blocks and custom (parenthesized) options.
pub fn without_extensions(src: &str) -> Option<String> {
    fn opts(o: &mut Vec<Opt>) {
        o.retain(|x| !x.name.starts_with('('));
    }
    fn fields(fs: &mut [Field]) {
        for f in fs {
            opts(&mut f.options);
        }
    }
    fn types(ts: &mut [TypeEl]) {
        for t in ts {
            match t {
                TypeEl::Message(m) => {
                    m.extensions.clear();
                    m.extends.clear();
                    opts(&mut m.options);
                    fields(&mut m.fields);
                    for o in &mut m.oneofs {
                        opts(&mut o.options);
                        fields(&mut o.fields);
                    }
                    types(&mut m.nested);
                }
                TypeEl::Enum(e) => {
                    opts(&mut e.options);
                    for c in &mut e.constants {
                        opts(&mut c.2);
                    }
                }
            }
        }
    }
    let tokens = tokenize(src)?;
    let mut file = Parser { t: tokens, i: 0 }.file()?;
    file.extends.clear();
    opts(&mut file.options);
    types(&mut file.types);
    for sv in &mut file.services {
        opts(&mut sv.options);
        for r in &mut sv.rpcs {
            opts(&mut r.options);
        }
    }
    Some(file_str(&file))
}

/// Wire-style canonical text for a `.proto` source, or `None` if the source
/// uses something this model doesn't cover.
pub fn canonical(src: &str) -> Option<String> {
    let tokens = tokenize(src)?;
    let file = Parser { t: tokens, i: 0 }.file()?;
    Some(file_str(&file))
}

// ---------------------------------------------------------------------------
// Schema tags
// ---------------------------------------------------------------------------

/// Add or remove `(confluent.message_meta)` / `(confluent.field_meta)` tags at
/// the paths named by the edits (`M`, `M.N`, `M.a`, `M.N.x`), like Confluent's
/// `ProtobufSchema#copy(tagsToAdd, tagsToRemove)`.
pub fn apply_tags(src: &str, add: &[super::tags::TagEdit], remove: &[super::tags::TagEdit]) -> Result<String, String> {
    let tokens = tokenize(src).ok_or_else(|| "Could not parse Protobuf".to_string())?;
    let mut file = Parser { t: tokens, i: 0 }.file().ok_or_else(|| "Could not parse Protobuf".to_string())?;
    for edit in add.iter().map(|e| (e, true)).chain(remove.iter().map(|e| (e, false))) {
        let (e, adding) = edit;
        let segments: Vec<&str> = e.path.split('.').collect();
        if !edit_types(&mut file.types, &segments, e, adding) {
            // Confluent names the segment that could not be resolved.
            let (what, name) = match (e.record, segments.as_slice()) {
                (false, [.., field]) if segments.len() > 1 => ("Field", *field),
                (_, [first, ..]) => ("Message", *first),
                _ => ("Message", e.path.as_str()),
            };
            return Err(format!("java.lang.IllegalArgumentException: No matching {what} with name '{name}' found in the schema"));
        }
    }
    Ok(file_str(&file))
}

/// Merge or remove tags in an option list (`tags: [..]` inside the meta option).
fn edit_option_tags(options: &mut Vec<Opt>, name: &str, tags: &[String], adding: bool) {
    let mut current: Vec<String> = options
        .iter()
        .find(|o| o.name == name)
        .and_then(|o| match &o.value {
            Value::Map(entries) => entries.iter().find(|(k, _)| k == "tags").map(|(_, v)| v),
            _ => None,
        })
        .map(|v| match v {
            Value::List(items) => items.iter().filter_map(|x| match x {
                Value::Str(s) => Some(s.clone()),
                _ => None,
            }).collect(),
            Value::Str(s) => vec![s.clone()],
            _ => Vec::new(),
        })
        .unwrap_or_default();
    if adding {
        for t in tags {
            if !current.contains(t) {
                current.push(t.clone());
            }
        }
    } else {
        current.retain(|t| !tags.contains(t));
    }
    options.retain(|o| o.name != name);
    if !current.is_empty() {
        let list = Value::List(current.into_iter().map(Value::Str).collect());
        options.push(Opt { name: name.to_string(), value: Value::Map(vec![("tags".to_string(), list)]) });
    }
}

fn edit_types(types: &mut [TypeEl], segments: &[&str], edit: &super::tags::TagEdit, adding: bool) -> bool {
    let Some((head, rest)) = segments.split_first() else { return false };
    for t in types.iter_mut() {
        match t {
            TypeEl::Message(m) if m.name == *head => {
                if rest.is_empty() {
                    if !edit.record {
                        return false;
                    }
                    edit_option_tags(&mut m.options, "(confluent.message_meta)", &edit.tags, adding);
                    return true;
                }
                if rest.len() == 1 && !edit.record {
                    if let Some(f) = m.fields.iter_mut().find(|f| f.name == rest[0]) {
                        edit_option_tags(&mut f.options, "(confluent.field_meta)", &edit.tags, adding);
                        return true;
                    }
                    for o in m.oneofs.iter_mut() {
                        if let Some(f) = o.fields.iter_mut().find(|f| f.name == rest[0]) {
                            edit_option_tags(&mut f.options, "(confluent.field_meta)", &edit.tags, adding);
                            return true;
                        }
                    }
                }
                return edit_types(&mut m.nested, rest, edit, adding);
            }
            TypeEl::Enum(e) if e.name == *head && rest.is_empty() && edit.record => {
                edit_option_tags(&mut e.options, "(confluent.enum_meta)", &edit.tags, adding);
                return true;
            }
            _ => {}
        }
    }
    false
}

// ---------------------------------------------------------------------------
// `format=serialized`: Confluent's `ProtobufSchema#toDynamicSchema`
// ---------------------------------------------------------------------------

/// The `FileDescriptorProto` Confluent serves for `format=serialized`, built
/// from the Wire model like `toDynamicSchema` does (not what protoc would
/// produce): type names stay as written, a field's type is only set when the
/// name resolves to a known message or enum, proto3 singular fields carry no
/// label, map entries are `<field>Entry` messages, streaming flags are always
/// set. `pool` is the compiled schema, used to resolve type names.
pub fn serialized(src: &str, pool: &DescriptorPool, name: &str) -> Option<Vec<u8>> {
    use prost::Message as _;
    let tokens = tokenize(src)?;
    let file = Parser { t: tokens, i: 0 }.file()?;
    let proto3 = file.syntax.as_deref() == Some("proto3");
    let pkg = file.package.clone().unwrap_or_default();
    let b = DescBuilder { pool, proto3 };
    let mut fd = prost_types::FileDescriptorProto {
        name: Some(name.to_string()),
        package: file.package.clone(),
        syntax: file.syntax.clone(),
        ..Default::default()
    };
    for t in &file.types {
        match t {
            TypeEl::Message(m) => fd.message_type.push(b.message(m, &pkg)),
            TypeEl::Enum(e) => fd.enum_type.push(b.enumeration(e)),
        }
    }
    for s in &file.services {
        fd.service.push(b.service(s));
    }
    for x in &file.extends {
        for f in &x.fields {
            let mut field = b.field(f, &f.ty, f.label.clone(), &pkg);
            field.extendee = Some(x.name.clone());
            fd.extension.push(field);
        }
    }
    let known = |imp: &str| pool.get_file_by_name(imp).is_some();
    for imp in &file.imports {
        if known(imp) && !fd.dependency.contains(imp) {
            fd.dependency.push(imp.clone());
        }
    }
    for imp in &file.public_imports {
        if known(imp) {
            if !fd.dependency.contains(imp) {
                fd.dependency.push(imp.clone());
            }
            let idx = fd.dependency.iter().position(|d| d == imp).unwrap_or(0) as i32;
            fd.public_dependency.push(idx);
        }
    }
    fd.options = file_options(&file.options);
    Some(fd.encode_to_vec())
}

struct DescBuilder<'a> {
    pool: &'a DescriptorPool,
    proto3: bool,
}

const SCALARS: &[(&str, i32)] = &[
    ("double", 1),
    ("float", 2),
    ("int64", 3),
    ("uint64", 4),
    ("int32", 5),
    ("fixed64", 6),
    ("fixed32", 7),
    ("bool", 8),
    ("string", 9),
    ("bytes", 12),
    ("uint32", 13),
    ("sfixed32", 15),
    ("sfixed64", 16),
    ("sint32", 17),
    ("sint64", 18),
];

fn opt<'a>(opts: &'a [Opt], name: &str) -> Option<&'a Value> {
    // mergeOptions: the last one wins; a leading dot is ignored.
    opts.iter().rev().find(|o| o.name.trim_start_matches('.') == name).map(|o| &o.value)
}

fn opt_text(v: &Value) -> String {
    match v {
        Value::Str(s) | Value::Raw(s) => s.clone(),
        other => value_str(other),
    }
}

fn opt_bool(opts: &[Opt], name: &str) -> Option<bool> {
    opt(opts, name).map(|v| opt_text(v) == "true")
}

fn parse_int(s: &str) -> Option<i32> {
    let (neg, t) = match s.strip_prefix('-') {
        Some(t) => (true, t),
        None => (false, s),
    };
    let v = if let Some(h) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        i64::from_str_radix(h, 16).ok()?
    } else if t.len() > 1 && t.starts_with('0') {
        i64::from_str_radix(&t[1..], 8).ok()?
    } else {
        t.parse::<i64>().ok()?
    };
    i32::try_from(if neg { -v } else { v }).ok()
}

/// Reserved/extension statements as (start, end-inclusive) ranges and names.
fn ranges(stmts: &[String], kw: &str) -> (Vec<(i32, i32)>, Vec<String>) {
    let mut nums = Vec::new();
    let mut names = Vec::new();
    for s in stmts {
        let body = s.trim_start_matches(kw).trim().trim_end_matches(';');
        for part in body.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            if let Some(n) = part.strip_prefix('"') {
                names.push(n.trim_end_matches('"').to_string());
            } else if let Some((a, b)) = part.split_once(" to ") {
                let end = if b.trim() == "max" { 536_870_911 } else { parse_int(b.trim()).unwrap_or(0) };
                nums.push((parse_int(a.trim()).unwrap_or(0), end));
            } else if let Some(n) = parse_int(part) {
                nums.push((n, n));
            }
        }
    }
    (nums, names)
}

#[allow(deprecated)] // java_generate_equals_and_hash is still settable in .proto files
fn file_options(opts: &[Opt]) -> Option<prost_types::FileOptions> {
    let mut o = prost_types::FileOptions::default();
    let mut any = false;
    let mut set_s = |name: &str, slot: &mut Option<String>| {
        if let Some(v) = opt(opts, name) {
            *slot = Some(opt_text(v));
            any = true;
        }
    };
    set_s("java_package", &mut o.java_package);
    set_s("java_outer_classname", &mut o.java_outer_classname);
    set_s("go_package", &mut o.go_package);
    set_s("objc_class_prefix", &mut o.objc_class_prefix);
    set_s("csharp_namespace", &mut o.csharp_namespace);
    set_s("swift_prefix", &mut o.swift_prefix);
    set_s("php_class_prefix", &mut o.php_class_prefix);
    set_s("php_namespace", &mut o.php_namespace);
    set_s("php_metadata_namespace", &mut o.php_metadata_namespace);
    set_s("ruby_package", &mut o.ruby_package);
    let mut set_b = |name: &str, slot: &mut Option<bool>| {
        if let Some(b) = opt_bool(opts, name) {
            *slot = Some(b);
            any = true;
        }
    };
    set_b("java_multiple_files", &mut o.java_multiple_files);
    set_b("java_generate_equals_and_hash", &mut o.java_generate_equals_and_hash);
    set_b("java_string_check_utf8", &mut o.java_string_check_utf8);
    set_b("cc_generic_services", &mut o.cc_generic_services);
    set_b("java_generic_services", &mut o.java_generic_services);
    set_b("py_generic_services", &mut o.py_generic_services);
    set_b("deprecated", &mut o.deprecated);
    set_b("cc_enable_arenas", &mut o.cc_enable_arenas);
    if let Some(v) = opt(opts, "optimize_for") {
        o.optimize_for = match opt_text(v).as_str() {
            "SPEED" => Some(1),
            "CODE_SIZE" => Some(2),
            "LITE_RUNTIME" => Some(3),
            _ => None,
        };
        any |= o.optimize_for.is_some();
    }
    any.then_some(o)
}

impl DescBuilder<'_> {
    /// Confluent's `Context#resolveFull`: the written name, looked up from the
    /// innermost scope outwards.
    fn kind_of(&self, ty: &str, scope: &str) -> Option<i32> {
        let found = |full: &str| {
            if self.pool.get_message_by_name(full).is_some() {
                Some(11)
            } else if self.pool.get_enum_by_name(full).is_some() {
                Some(14)
            } else {
                None
            }
        };
        if let Some(abs) = ty.strip_prefix('.') {
            return found(abs);
        }
        let mut scope = scope.to_string();
        loop {
            let candidate = if scope.is_empty() { ty.to_string() } else { format!("{scope}.{ty}") };
            if let Some(k) = found(&candidate) {
                return Some(k);
            }
            if scope.is_empty() {
                return None;
            }
            scope = scope.rfind('.').map(|i| scope[..i].to_string()).unwrap_or_default();
        }
    }

    fn field(&self, f: &Field, ty: &str, label: Option<String>, scope: &str) -> prost_types::FieldDescriptorProto {
        let mut fd = prost_types::FieldDescriptorProto {
            name: Some(f.name.clone()),
            number: parse_int(&f.tag),
            ..Default::default()
        };
        match SCALARS.iter().find(|(n, _)| *n == ty) {
            Some((_, t)) => fd.r#type = Some(*t),
            None => {
                fd.r#type = self.kind_of(ty, scope);
                fd.type_name = Some(ty.to_string());
            }
        }
        fd.label = match label.as_deref() {
            Some("optional") => Some(1),
            Some("required") => Some(2),
            Some("repeated") => Some(3),
            _ => None,
        };
        if let Some(d) = opt(&f.options, "default") {
            fd.default_value = Some(opt_text(d));
        }
        if let Some(j) = opt(&f.options, "json_name") {
            fd.json_name = Some(opt_text(j));
        }
        let mut o = prost_types::FieldOptions::default();
        let mut any = false;
        if let Some(v) = opt(&f.options, "ctype") {
            o.ctype = match opt_text(v).as_str() {
                "STRING" => Some(0),
                "CORD" => Some(1),
                "STRING_PIECE" => Some(2),
                _ => None,
            };
            any = true;
        }
        if let Some(b) = opt_bool(&f.options, "packed") {
            o.packed = Some(b);
            any = true;
        }
        if let Some(v) = opt(&f.options, "jstype") {
            o.jstype = match opt_text(v).as_str() {
                "JS_NORMAL" => Some(0),
                "JS_STRING" => Some(1),
                "JS_NUMBER" => Some(2),
                _ => None,
            };
            any = true;
        }
        if let Some(b) = opt_bool(&f.options, "deprecated") {
            o.deprecated = Some(b);
            any = true;
        }
        if any {
            fd.options = Some(o);
        }
        fd
    }

    fn message(&self, m: &Message, scope: &str) -> prost_types::DescriptorProto {
        let full = if scope.is_empty() { m.name.clone() } else { format!("{scope}.{}", m.name) };
        let mut d = prost_types::DescriptorProto { name: Some(m.name.clone()), ..Default::default() };
        for t in &m.nested {
            match t {
                TypeEl::Message(n) => d.nested_type.push(self.message(n, &full)),
                TypeEl::Enum(e) => d.enum_type.push(self.enumeration(e)),
            }
        }
        let mut added: Vec<&str> = Vec::new();
        for o in &m.oneofs {
            let idx = d.oneof_decl.len() as i32;
            d.oneof_decl.push(prost_types::OneofDescriptorProto {
                name: Some(o.name.clone()),
                options: Some(prost_types::OneofOptions::default()),
            });
            for f in &o.fields {
                let mut fd = self.field(f, &f.ty, Some("optional".into()), &full);
                fd.oneof_index = Some(idx);
                d.field.push(fd);
                added.push(&f.name);
            }
        }
        for f in &m.fields {
            if added.contains(&f.name.as_str()) {
                continue;
            }
            let mut label = f.label.clone();
            let mut ty = f.ty.clone();
            if let Some(inner) = f.ty.strip_prefix("map<").and_then(|t| t.strip_suffix('>'))
                && let Some((k, v)) = inner.split_once(',')
            {
                label = Some("repeated".into());
                ty = format!("{}Entry", f.name);
                let mut entry = prost_types::DescriptorProto {
                    name: Some(ty.clone()),
                    options: Some(prost_types::MessageOptions { map_entry: Some(true), ..Default::default() }),
                    ..Default::default()
                };
                let kv = |name: &str, t: &str, n: i32| {
                    let f = Field { name: name.into(), ty: t.trim().into(), tag: n.to_string(), ..Default::default() };
                    self.field(&f, t.trim(), None, &full)
                };
                entry.field.push(kv("key", k, 1));
                entry.field.push(kv("value", v, 2));
                d.nested_type.push(entry);
            }
            if label.as_deref() == Some("optional") && self.proto3 {
                let idx = d.oneof_decl.len() as i32;
                d.oneof_decl.push(prost_types::OneofDescriptorProto {
                    name: Some(format!("_{}", f.name)),
                    options: Some(prost_types::OneofOptions::default()),
                });
                let mut fd = self.field(f, &ty, Some("optional".into()), &full);
                fd.proto3_optional = Some(true);
                fd.oneof_index = Some(idx);
                d.field.push(fd);
                continue;
            }
            d.field.push(self.field(f, &ty, label, &full));
        }
        let (nums, names) = ranges(&m.reserved, "reserved");
        d.reserved_range = nums
            .into_iter()
            .map(|(a, b)| prost_types::descriptor_proto::ReservedRange { start: Some(a), end: Some(b + 1) })
            .collect();
        d.reserved_name = names;
        let (ext, _) = ranges(&m.extensions, "extensions");
        d.extension_range = ext
            .into_iter()
            .map(|(a, b)| prost_types::descriptor_proto::ExtensionRange {
                start: Some(a),
                end: Some(b + 1),
                options: Some(Default::default()),
            })
            .collect();
        for x in &m.extends {
            for f in &x.fields {
                let mut fd = self.field(f, &f.ty, f.label.clone(), &full);
                fd.extendee = Some(x.name.clone());
                d.extension.push(fd);
            }
        }
        let mut o = prost_types::MessageOptions::default();
        let mut any = false;
        if let Some(b) = opt_bool(&m.options, "no_standard_descriptor_accessor") {
            o.no_standard_descriptor_accessor = Some(b);
            any = true;
        }
        if let Some(b) = opt_bool(&m.options, "deprecated") {
            o.deprecated = Some(b);
            any = true;
        }
        if let Some(b) = opt_bool(&m.options, "map_entry") {
            o.map_entry = Some(b);
            any = true;
        }
        if any {
            d.options = Some(o);
        }
        d
    }

    fn enumeration(&self, e: &EnumEl) -> prost_types::EnumDescriptorProto {
        let mut d = prost_types::EnumDescriptorProto { name: Some(e.name.clone()), ..Default::default() };
        let mut o = prost_types::EnumOptions::default();
        let mut any = false;
        if let Some(b) = opt_bool(&e.options, "allow_alias") {
            o.allow_alias = Some(b);
            any = true;
        }
        if let Some(b) = opt_bool(&e.options, "deprecated") {
            o.deprecated = Some(b);
            any = true;
        }
        if any {
            d.options = Some(o);
        }
        let (nums, names) = ranges(&e.reserved, "reserved");
        d.reserved_range = nums
            .into_iter()
            .map(|(a, b)| prost_types::enum_descriptor_proto::EnumReservedRange { start: Some(a), end: Some(b) })
            .collect();
        d.reserved_name = names;
        for (name, tag, opts) in &e.constants {
            let mut v = prost_types::EnumValueDescriptorProto { name: Some(name.clone()), number: parse_int(tag), options: None };
            let mut vo = prost_types::EnumValueOptions::default();
            let mut any = false;
            if let Some(b) = opt_bool(opts, "deprecated") {
                vo.deprecated = Some(b);
                any = true;
            }
            if any {
                v.options = Some(vo);
            }
            d.value.push(v);
        }
        d
    }

    fn service(&self, s: &Service) -> prost_types::ServiceDescriptorProto {
        let mut d = prost_types::ServiceDescriptorProto { name: Some(s.name.clone()), ..Default::default() };
        if let Some(b) = opt_bool(&s.options, "deprecated") {
            d.options = Some(prost_types::ServiceOptions { deprecated: Some(b), ..Default::default() });
        }
        for r in &s.rpcs {
            let mut m = prost_types::MethodDescriptorProto {
                name: Some(r.name.clone()),
                input_type: Some(r.req.clone()),
                output_type: Some(r.resp.clone()),
                client_streaming: Some(r.req_stream),
                server_streaming: Some(r.resp_stream),
                options: None,
            };
            let mut o = prost_types::MethodOptions::default();
            let mut any = false;
            if let Some(b) = opt_bool(&r.options, "deprecated") {
                o.deprecated = Some(b);
                any = true;
            }
            if let Some(v) = opt(&r.options, "idempotency_level") {
                o.idempotency_level = match opt_text(v).as_str() {
                    "IDEMPOTENCY_UNKNOWN" => Some(0),
                    "NO_SIDE_EFFECTS" => Some(1),
                    "IDEMPOTENT" => Some(2),
                    _ => None,
                };
                any = true;
            }
            if any {
                m.options = Some(o);
            }
            d.method.push(m);
        }
        d
    }
}

#[cfg(test)]
mod tests {
    use super::canonical;

    #[test]
    fn formats_like_wire() {
        let src = "syntax=\"proto3\";package acme;// c\nmessage Order{string id=1;int32 qty=2;}";
        assert_eq!(canonical(src).unwrap(), "syntax = \"proto3\";\npackage acme;\n\nmessage Order {\n  string id = 1;\n  int32 qty = 2;\n}\n");
    }
}
