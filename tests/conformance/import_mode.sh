#!/bin/bash
# usage: probe_import.sh URL
U=$1; H='Content-Type: application/vnd.schemaregistry.v1+json'
p() { printf '%-58s %s\n' "$1" "$(curl -s -X "$2" -H "$H" "$U$3" ${4:+-d "$4"} | head -c 150)"; }
s() { printf '{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"%s\\",\\"fields\\":[{\\"name\\":\\"a\\",\\"type\\":\\"%s\\"}]}"%s}' "$1" "${2:-int}" "$3"; }
p "A1 register S in default"                         POST /subjects/ida/versions "$(s S)"
p "A2 same S in context .ca"                         POST /subjects/:.ca:ida/versions "$(s S)"
p "A3 other schema in .ca"                            POST /subjects/:.ca:idb/versions "$(s T)"
p "B0 READWRITE register with explicit id"            POST /subjects/rw1/versions "$(s R int ',"id":500')"
p "B1 IMPORT on empty subject"                        PUT /mode/imp1 '{"mode":"IMPORT"}'
p "B2 IMPORT on non-empty subject, no force"          PUT /mode/ida '{"mode":"IMPORT"}'
p "B3 global IMPORT, no force"                        PUT /mode '{"mode":"IMPORT"}'
p "B4 import id=100 v=1"                              POST /subjects/imp1/versions "$(s I1 int ',"id":100,"version":1')"
p "B5 repeat same"                                    POST /subjects/imp1/versions "$(s I1 int ',"id":100,"version":1')"
p "B6 different schema, same id=100"                  POST /subjects/imp1/versions "$(s I2 int ',"id":100,"version":2')"
p "B7 no id in IMPORT mode"                           POST /subjects/imp1/versions "$(s I3)"
p "B8 id=101, no version"                             POST /subjects/imp1/versions "$(s I4 int ',"id":101')"
p "B9 versions after B8"                              GET /subjects/imp1/versions
p "B10 id=102 version gap v=10"                       POST /subjects/imp1/versions "$(s I5 int ',"id":102,"version":10')"
p "B11 id=103 duplicate v=10"                         POST /subjects/imp1/versions "$(s I6 int ',"id":103,"version":10')"
p "B12 incompatible schema in IMPORT (compat skipped?)" POST /subjects/imp1/versions "$(s I1 string ',"id":104,"version":11')"
p "B13 same content as id 100, other id=200"         PUT /mode/imp2 '{"mode":"IMPORT"}'
p "     register"                                      POST /subjects/imp2/versions "$(s I1 int ',"id":200,"version":1')"
p "B14 id 200 lookup"                                 GET /schemas/ids/200
p "B15 id=1 (in use by ida) with other schema"        POST /subjects/imp2/versions "$(s X int ',"id":1,"version":2')"
p "B16 back to READWRITE"                             DELETE /mode/imp1
p "B17 new auto id after import"                       POST /subjects/imp1/versions "$(s I9 int ',"id":-1')"
p "B18 new auto id in other subject"                   POST /subjects/fresh/versions "$(s F1)"
p "B19 import into context .cb id=100"                PUT /mode/:.cb:imp '{"mode":"IMPORT"}'
p "     register id=100 (exists in default)"           POST /subjects/:.cb:imp/versions "$(s Z int ',"id":100,"version":1')"
p "B20 delete in import mode"                          DELETE /subjects/imp2/versions/1
