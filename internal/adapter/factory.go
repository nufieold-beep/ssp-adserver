package adapter

import "strings"

// RegisterFromConfig constructs and registers a supported adapter type.
// Returns false when config is nil, missing an ID, or has an unsupported type.
func RegisterFromConfig(reg *Registry, cfg *AdapterConfig) bool {
	if reg == nil || cfg == nil || strings.TrimSpace(cfg.ID) == "" {
		return false
	}

	switch normalizeAdapterType(cfg.Type) {
	case TypeORTB:
		cfg.Type = TypeORTB
		reg.Register(NewORTBAdapter(cfg), cfg)
		return true
	case TypeVAST:
		cfg.Type = TypeVAST
		reg.Register(NewVASTAdapter(cfg), cfg)
		return true
	default:
		return false
	}
}

func normalizeAdapterType(adapterType AdapterType) AdapterType {
	switch strings.ToLower(strings.TrimSpace(string(adapterType))) {
	case string(TypeORTB):
		return TypeORTB
	case string(TypeVAST):
		return TypeVAST
	default:
		return ""
	}
}
