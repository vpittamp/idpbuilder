package stacks

import (
	"reflect"
	"testing"
)

func TestRedactArgsMasksSensitiveValues(t *testing.T) {
	got := redactArgs([]string{
		"--password", "developer",
		"--token=abc123",
		"--from-literal=password=secret",
		"--from-literal=url=http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git",
		"https://user:secret@example.com/repo.git",
		"--cluster", "ryzen",
	})
	want := []string{
		"--password", "<redacted>",
		"--token=<redacted>",
		"--from-literal=password=<redacted>",
		"--from-literal=url=http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git",
		"<redacted-url>",
		"--cluster", "ryzen",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redacted args mismatch\nwant: %#v\n got: %#v", want, got)
	}
}
