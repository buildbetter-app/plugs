package model

import (
	"encoding/json"
)

type PrincipalRef struct {
	Kind    string         `json:"kind"` // anonymous, user, agent, service, plugin, system
	ID      string         `json:"id"`
	Sponsor *PrincipalRef  `json:"sponsor,omitempty" wire:"nullable"`
	Caveats map[string]any `json:"caveats,omitempty"`
}

func (p *PrincipalRef) UnmarshalJSON(data []byte) error {
	type wirePrincipal PrincipalRef
	var value wirePrincipal
	if err := decodeStrict(data, &value, "kind", "id"); err != nil {
		return err
	}
	if err := validatePrincipal(PrincipalRef(value)); err != nil {
		return err
	}
	*p = PrincipalRef(value)
	return nil
}

func (p PrincipalRef) MarshalJSON() ([]byte, error) {
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	type wirePrincipal PrincipalRef
	return json.Marshal(wirePrincipal(p))
}
