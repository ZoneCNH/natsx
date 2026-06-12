package natsx

import (
	"strconv"
	"strings"
	"unicode"
)

type SubjectBuilder struct{ parts []string }

type SubjectParts struct {
	Domain   string
	Resource string
	Action   string
	Version  int
}

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

func (b SubjectBuilder) Build(domain, resource, action string, version int) (string, error) {
	return BuildSubject(domain, resource, action, version)
}

func (b SubjectBuilder) Parse(subject string) (SubjectParts, error) {
	return ParseSubject(subject)
}

func BuildSubject(domain, resource, action string, version int) (string, error) {
	const op = "natsx.BuildSubject"
	if version <= 0 {
		return "", validationError(op, "version must be positive", nil)
	}
	parts := []string{domain, resource, action}
	for _, part := range parts {
		if err := validateSubjectToken(op, part); err != nil {
			return "", err
		}
	}
	return strings.Join([]string{
		strings.TrimSpace(domain),
		strings.TrimSpace(resource),
		strings.TrimSpace(action),
		"v" + strconv.Itoa(version),
	}, "."), nil
}

func ParseSubject(subject string) (SubjectParts, error) {
	const op = "natsx.ParseSubject"
	if err := ValidateSubject(op, subject); err != nil {
		return SubjectParts{}, err
	}
	parts := strings.Split(subject, ".")
	if len(parts) != 4 {
		return SubjectParts{}, validationError(op, "subject must have domain.resource.action.vN shape", nil)
	}
	for _, part := range parts[:3] {
		if err := validateSubjectToken(op, part); err != nil {
			return SubjectParts{}, err
		}
	}
	versionPart := parts[3]
	if !strings.HasPrefix(versionPart, "v") || len(versionPart) == 1 {
		return SubjectParts{}, validationError(op, "subject version must use vN shape", nil)
	}
	version, err := strconv.Atoi(versionPart[1:])
	if err != nil || version <= 0 {
		return SubjectParts{}, validationError(op, "subject version must be positive", err)
	}
	return SubjectParts{Domain: parts[0], Resource: parts[1], Action: parts[2], Version: version}, nil
}

func ValidateSubject(op, subject string) error {
	if strings.TrimSpace(subject) == "" {
		return validationError(op, "subject is required", nil)
	}
	if strings.ContainsFunc(subject, unicode.IsSpace) {
		return validationError(op, "subject must not contain whitespace", nil)
	}
	if strings.HasPrefix(subject, ".") || strings.HasSuffix(subject, ".") || strings.Contains(subject, "..") {
		return validationError(op, "subject must not contain empty tokens", nil)
	}
	return nil
}

func validateSubjectToken(op, token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return validationError(op, "subject token is required", nil)
	}
	if strings.Contains(token, ".") {
		return validationError(op, "subject token must not contain dots", nil)
	}
	if strings.ContainsAny(token, "*>") {
		return validationError(op, "subject token must not contain wildcards", nil)
	}
	if strings.ContainsFunc(token, unicode.IsSpace) {
		return validationError(op, "subject token must not contain whitespace", nil)
	}
	return nil
}
