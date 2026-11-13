package access

import (
	"errors"
	"testing"
)

func TestFindIdentity(t *testing.T) {
	identities := []Identity{{ID: "a1", Label: "admin"}, {ID: "a2", Label: "kafka3o-cli-test"}, {ID: "c1", Label: "Admin"}}
	if identity, err := FindIdentity(identities, "KAFKA3O-cli-test"); err != nil || identity.ID != "a2" {
		t.Fatalf("label: %+v %v", identity, err)
	}
	if identity, err := FindIdentity(identities, "c1"); err != nil || identity.Label != "Admin" {
		t.Fatalf("id: %+v %v", identity, err)
	}
	var missing ErrIdentityNotFound
	if _, err := FindIdentity(identities, "admin"); !errors.As(err, &missing) || !missing.Ambiguous {
		t.Fatalf("ambiguous: %v", err)
	}
	if _, err := FindIdentity(identities, "nobody"); err == nil || err.Error() != "no identity has the id or label nobody" {
		t.Fatalf("missing: %v", err)
	}
}
