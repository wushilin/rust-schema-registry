#!/bin/bash
U=$1; H='Content-Type: application/vnd.schemaregistry.v1+json'
p() { printf '%-62s %s\n' "$1" "$(curl -s -X "$2" -H "$H" "$U$3" ${4:+-d "$4"} | head -c 120)"; }
s() { printf '{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"%s\\",\\"fields\\":[{\\"name\\":\\"a\\",\\"type\\":\\"int\\"}]}"%s}' "$1" "$2"; }
p "setup: IMPORT mode on :.xa:s1, :.xb:s1, s1-default"   PUT /mode/:.xa:s1 '{"mode":"IMPORT"}'
curl -s -X PUT -H "$H" $U/mode/:.xb:s1 -d '{"mode":"IMPORT"}' >/dev/null
curl -s -X PUT -H "$H" $U/mode/xdef-s1 -d '{"mode":"IMPORT"}' >/dev/null
p "C1 import id 777 = schema A into .xa"                  POST /subjects/:.xa:s1/versions "$(s A ',"id":777,"version":1')"
p "C2 import id 777 = schema B (different) into .xb"      POST /subjects/:.xb:s1/versions "$(s B ',"id":777,"version":1')"
p "C3 import id 777 = schema C into default context"      POST /subjects/xdef-s1/versions "$(s C ',"id":777,"version":1')"
p "C4 GET /schemas/ids/777 (no subject)"                  GET /schemas/ids/777
p "C5 GET /schemas/ids/777?subject=:.xa:"                 GET "/schemas/ids/777?subject=:.xa:"
p "C6 GET /schemas/ids/777?subject=:.xb:"                 GET "/schemas/ids/777?subject=:.xb:"
p "C7 GET /schemas/ids/777?subject=s1 (unqualified)"      GET "/schemas/ids/777?subject=s1"
p "C8 /schemas/ids/777/versions"                          GET /schemas/ids/777/versions
p "C9 READWRITE .xa: next auto id"                        DELETE /mode/:.xa:s1
p "     register new in .xa"                              POST /subjects/:.xa:s2/versions "$(s D)"
