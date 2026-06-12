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
	if err := ValidateSubject("test", "orders created"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(invalid) error = %v, want validation", err)
	}
}
