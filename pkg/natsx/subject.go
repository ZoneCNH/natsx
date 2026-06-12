package natsx

import "strings"

type SubjectBuilder struct{ parts []string }

func Subject(parts ...string) SubjectBuilder { return SubjectBuilder{}.Append(parts...) }
func (b SubjectBuilder) Append(parts ...string) SubjectBuilder {
	for _, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), ".")
		if p != "" {
			b.parts = append(b.parts, p)
		}
	}
	return b
}
func (b SubjectBuilder) String() string { return strings.Join(b.parts, ".") }
func ValidateSubject(op, subject string) error {
	if strings.TrimSpace(subject) == "" {
		return validationError(op, "subject is required", nil)
	}
	if strings.ContainsAny(subject, " \t\r\n") {
		return validationError(op, "subject must not contain whitespace", nil)
	}
	return nil
}
