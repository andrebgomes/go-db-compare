package internal

import (
	"context"
	"fmt"
	"go-db-compare/configs"
)

type contextKey string

const (
	// List of available strategies
	strategyDumps1 = "dump"
	strategyDumps2 = "twodumps"
	strategyLive   = "live"
	strategyDiff   = "diff"

	// List of keys to use when storing values in the context
	contextKeyConfig contextKey = "config"
)

var (
	// strategies is a map containing the valid strategies. Used for strategy validation.
	strategies = map[string]bool{
		strategyDumps1: true,
		strategyDumps2: true,
		strategyLive:   true,
		strategyDiff:   true,
	}
)

// RunCompare is responsible for running the process according to given strategy.
func RunCompare(config *configs.Conf, strategy string) error {
	// Validate given strategy
	if !isValidStrategy(strategy) {
		return fmt.Errorf("strategy not valid")
	}

	// Initiate context with given config
	ctx := context.WithValue(context.Background(), contextKeyConfig, config)

	// Run the comparison according to the strategy
	var err error
	switch strategy {
	case strategyDumps1:
		err = runStrategyDump1(ctx)
	case strategyDumps2:
		err = runStrategyDump2(ctx)
	case strategyLive:
		err = runStrategyLive(ctx)
	case strategyDiff:
		err = runStrategyDiff(ctx)
	}

	if err != nil {
		return err
	}

	return nil
}

// isValidStrategy returns true if given strategy is valid.
func isValidStrategy(s string) bool {
	return strategies[s]
}

// getConfigFromContext returns the Conf existing in the given context.
func getConfigFromContext(ctx context.Context) *configs.Conf {
	return ctx.Value(contextKeyConfig).(*configs.Conf)
}
