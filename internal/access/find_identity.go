package access

import "strings"

// FindIdentity picks an identity by its exact id, or by its label compared case-insensitively.
func FindIdentity(identities []Identity, wanted string) (Identity, error) {
	matches := []Identity{}
	for _, identity := range identities {
		if identity.ID == wanted {
			return identity, nil
		}
		if strings.EqualFold(identity.Label, wanted) {
			matches = append(matches, identity)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return Identity{}, ErrIdentityNotFound{Identity: wanted}
	default:
		return Identity{}, ErrIdentityNotFound{Identity: wanted, Ambiguous: true}
	}
}
