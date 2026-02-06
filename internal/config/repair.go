package config

import (
	"errors"
	"strconv"
)

type RepairStrategy string

const (
	RepairStrategyPerFile    RepairStrategy = "per_file"
	RepairStrategyPerTorrent RepairStrategy = "per_torrent"
)

type Repair struct {
	Enabled     bool           `json:"enabled,omitempty"`
	Interval    string         `json:"interval,omitempty"`
	AutoProcess bool           `json:"auto_process,omitempty"`
	Workers     int            `json:"workers,omitempty"`
	Strategy    RepairStrategy `json:"strategy,omitempty"`
}

func (r Repair) IsZero() bool {
	return !r.Enabled && r.Interval == "" && !r.AutoProcess && r.Workers == 0 && r.Strategy == ""
}

func (c *Config) applyRepairEnvVars() {
	// Repair settings
	if val := getEnv("REPAIR__ENABLED"); val != "" {
		c.Repair.Enabled = parseBool(val)
	}
	if val := getEnv("REPAIR__INTERVAL"); val != "" {
		c.Repair.Interval = val
	}
	if val := getEnv("REPAIR__WORKERS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.Repair.Workers = v
		}
	}
	if val := getEnv("REPAIR__STRATEGY"); val != "" {
		c.Repair.Strategy = RepairStrategy(val)
	}
	if val := getEnv("REPAIR__AUTO_PROCESS"); val != "" {
		c.Repair.AutoProcess = parseBool(val)
	}
}

func (c *Config) validateRepair() error {
	if !c.Repair.Enabled {
		return nil
	}
	if c.Repair.Interval == "" {
		return errors.New("repair interval is required")
	}
	return nil
}
