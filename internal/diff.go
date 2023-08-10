package internal

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
)

func runStrategyDiff(ctx context.Context) error {
	config := getConfigFromContext(ctx)
	// Check if dirs exist
	if err := isDirValid(config.Dir); err != nil {
		return err
	}
	if err := isDirValid(config.Dir2); err != nil {
		return err
	}

	// Execute Ndiff shell script
	var detailArg string
	if detailArg = ""; config.Detailed {
		detailArg = "-d"
	}

	cmd, err := exec.Command("./Ndiff.sh", "-l", strconv.Itoa(config.Limit),
		"-D", config.Dir, "-D", config.Dir2, detailArg).Output()
	if err != nil {
		fmt.Printf("error %s", err)
	}
	fmt.Print(string(cmd))

	return nil
}
