#!/bin/bash
# Per-context id counters: no cross-context collision avoidance, imports don't leak.
U=$1; H='Content-Type: application/vnd.schemaregistry.v1+json'
p() { printf '%-50s %s\n' "$1" "$(curl -s -X "$2" -H "$H" "$U$3" ${4:+-d "$4"} | head -c 110)"; }
s() { printf '{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"%s\\",\\"fields\\":[{\\"name\\":\\"a\\",\\"type\\":\\"int\\"}]}"%s}' "$1" "$2"; }
p "new schema in default"                    POST /subjects/d-one/versions "$(s D1)"
p "first schema in new context .zz"          POST /subjects/:.zz:one/versions "$(s Z1)"
p "second schema in .zz"                     POST /subjects/:.zz:two/versions "$(s Z2)"
p "new schema in default"                    POST /subjects/d-two/versions "$(s D2)"
p "IMPORT mode on :.yy:imp"                  PUT /mode/:.yy:imp '{"mode":"IMPORT"}'
p "import id 900 into .yy"                   POST /subjects/:.yy:imp/versions "$(s Y1 ',"id":900,"version":1')"
p "new schema in default after import"       POST /subjects/d-three/versions "$(s D3)"
p "new schema in .zz after import"           POST /subjects/:.zz:three/versions "$(s Z3)"
p "first schema in fresh context .ww"        POST /subjects/:.ww:one/versions "$(s W1)"
p "id 1 via subject 'one' (.ww and .zz tie)" GET "/schemas/ids/1?subject=one"
