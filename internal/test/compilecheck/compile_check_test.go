// Copyright (C) MongoDB, Inc. 2025-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"golang.org/x/mod/semver"
)

// TODO(GODRIVER-3515): This module cannot be included in the workspace since it
// requires a version of klauspost/compress that is not compatible with the Go
// Driver. Must use GOWORK=off to run this test.

const minSupportedVersion = "1.18"

func TestCompileCheck(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	rootDir := filepath.Dir(filepath.Dir(filepath.Dir(cwd)))

	versions, err := getDockerGolangImages()
	require.NoError(t, err)

	for _, version := range versions {
		version := version // Capture range variable.

		image := fmt.Sprintf("golang:%s", version)
		t.Run(image, func(t *testing.T) {
			t.Parallel()

			req := testcontainers.ContainerRequest{
				Image: image,
				Cmd:   []string{"tail", "-f", "/dev/null"},
				Mounts: []testcontainers.ContainerMount{
					testcontainers.BindMount(rootDir, "/workspace"),
				},
				WorkingDir: "/workspace",
				Env: map[string]string{
					"GC": "go",
					// Compilation modules are not part of the workspace as testcontainers requires
					// a version of klauspost/compress not supported by the Go Driver / other modules
					// in the workspace.
					"GOWORK": "off",
				},
			}

			genReq := testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			}

			container, err := testcontainers.GenericContainer(context.Background(), genReq)
			require.NoError(t, err)

			defer func() {
				err := container.Terminate(context.Background())
				require.NoError(t, err)
			}()

			exitCode, outputReader, err := container.Exec(context.Background(), []string{"bash", "etc/compile_check.sh"})
			require.NoError(t, err)

			output, err := io.ReadAll(outputReader)
			require.NoError(t, err)

			t.Logf("output: %s", output)
			assert.Equal(t, 0, exitCode)
		})
	}
}

// getDockerGolangImages retrieves the available Golang Docker image tags from
// Docker Hub that are considered valid and meet the specified version
// condition. It returns a slice of version strings in the MajorMinor format and
// an error, if any.
func getDockerGolangImages() ([]string, error) {
	// URL to fetch the Golang tags from Docker Hub with a page size of 100
	// records.
	var url = "https://hub.docker.com/v2/repositories/library/golang/tags?page_size=100"

	versionSet := map[string]bool{}
	versions := []string{}

	// Iteratively fetch and process tags from Docker Hub as long as there is a
	// valid next page URL.
	for url != "" {
		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to get response from Docker Hub: %w", err)
		}

		var data struct {
			Results []struct {
				Name string `json:"name"`
			} `json:"results"`
			Next string `json:"next"` // URL of the next page for pagination.
		}

		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return nil, fmt.Errorf("failed to decode response Body from Docker Hub: %w", err)
		}

		resp.Body.Close()

		for _, tag := range data.Results {
			// Skip tags that don't start with a digit (typically version numbers).
			if len(tag.Name) == 0 || tag.Name[0] < '0' || tag.Name[0] > '9' {
				continue
			}

			// Split the tag name and extract the base version part.
			// This handles tags like `1.18.1-alpine` by extracting `1.18.1`.
			base := strings.Split(tag.Name, "-")[0]

			// Reduce the base version to MajorMinor format (e.g., `v1.18`).
			baseMajMin := semver.MajorMinor("v" + base)
			if !semver.IsValid(baseMajMin) || versionSet[baseMajMin] {
				continue
			}

			if semver.Compare(baseMajMin, "v"+minSupportedVersion) >= 0 {
				versionSet[baseMajMin] = true
				versions = append(versions, baseMajMin[1:])
			}
		}

		// Move to the next page of results, set by the `Next` field.
		url = data.Next
	}

	return versions, nil
}
