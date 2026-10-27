// Command testforge is the headless CLI for GitOps-native pipeline execution
// in CI/CD. It parses declarative pipeline definitions (see
// examples/pipelines/checkout-load-test.yaml), submits them to
// pipeline-api-gateway, and maps quality-gate results back to process exit
// codes so GitHub Actions/GitLab CI/Jenkins fail the build on a regression.
package main

import (
	"os"

	"github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
