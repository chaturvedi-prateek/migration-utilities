package main

import (
	"regexp"
	"strings"
)

var credRE = regexp.MustCompile(`://([^:/@]+):[^@]*@`)

// redact hides the password in a mongodb URI for safe logging.
func redact(uri string) string {
	return credRE.ReplaceAllString(uri, "://$1:****@")
}

// stripLineComments removes // line comments so config files can be annotated.
// It is quote-aware so // inside a JSON string value (e.g. a mongodb:// URI) is
// preserved.
func stripLineComments(b []byte) []byte {
	var out strings.Builder
	in := string(b)
	inStr := false
	esc := false
	for i := 0; i < len(in); i++ {
		c := in[i]
		if inStr {
			out.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			out.WriteByte(c)
			continue
		}
		if c == '/' && i+1 < len(in) && in[i+1] == '/' {
			for i < len(in) && in[i] != '\n' {
				i++
			}
			if i < len(in) {
				out.WriteByte('\n')
			}
			continue
		}
		out.WriteByte(c)
	}
	return []byte(out.String())
}
