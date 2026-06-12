package natsx

import "testing"

func TestSubjectBuilderTrimsEmptyAndDots(t *testing.T) {
	got := Subject(" orders ", ".created.").Append("", "v1").String()
	if got != "orders.created.v1" {
		t.Fatalf("Subject().String() = %q", got)
	}
}

func TestValidateSubject(t *testing.T) {
	if err := ValidateSubject("test", "orders.created"); err != nil {
		t.Fatalf("ValidateSubject(valid) error = %v", err)
	}
	if err := ValidateSubject("test", ".orders.created"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(leading dot) error = %v, want validation", err)
	}
	if err := ValidateSubject("test", "orders..created"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(empty token) error = %v, want validation", err)
	}
	if err := ValidateSubject("test", "orders created"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(invalid) error = %v, want validation", err)
	}
}

func TestBuildAndParseSubject(t *testing.T) {
	subject, err := Subject().Build("orders", "created", "publish", 1)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if subject != "orders.created.publish.v1" {
		t.Fatalf("Build() subject = %q, want orders.created.publish.v1", subject)
	}

	parts, err := Subject().Parse(subject)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parts != (SubjectParts{Domain: "orders", Resource: "created", Action: "publish", Version: 1}) {
		t.Fatalf("Parse() = %+v", parts)
	}
}

func TestBuildSubjectRejectsInvalidCanonicalTokens(t *testing.T) {
	if _, err := BuildSubject("orders", "created", "publish", 0); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("BuildSubject(version=0) error = %v, want validation", err)
	}
	if _, err := BuildSubject("orders", "created.*", "publish", 1); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("BuildSubject(wildcard token) error = %v, want validation", err)
	}
}

func TestParseSubjectRejectsNonCanonicalShape(t *testing.T) {
	if _, err := ParseSubject("orders.created.v1"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(short) error = %v, want validation", err)
	}
	if _, err := ParseSubject("orders.created.publish.v0"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(v0) error = %v, want validation", err)
	}
	if _, err := ParseSubject("orders.created.publish.one"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(non-version) error = %v, want validation", err)
	}
}
