package topology

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"testing"

	"github.com/mongodb/mongo-go-driver/mongo/connstring"
	"github.com/mongodb/mongo-go-driver/mongo/internal/testutil/helpers"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/addr"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/description"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/result"
	"github.com/stretchr/testify/require"
)

type response struct {
	Host     string
	IsMaster result.IsMaster
}

type server struct {
	Type    string
	SetName string
}

type outcome struct {
	Servers      map[string]server
	TopologyType string
	SetName      string
	Compatible   *bool
}

type phase struct {
	Responses []response
	Outcome   outcome
}

type testCase struct {
	Description string
	URI         string
	Phases      []phase
}

const testsDir string = "../../../../data/server-discovery-and-monitoring/"

func (r *response) UnmarshalJSON(buf []byte) error {
	tmp := []interface{}{&r.Host, &r.IsMaster}
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}

	if len(tmp) != 2 {
		return fmt.Errorf("'response' JSON array must have exactly two elements")
	}

	return nil
}

func setUpFSM(t *testing.T, uri string) *fsm {
	fsm := newFSM()

	cs, err := connstring.Parse(uri)
	require.NoError(t, err)

	fsm.SetName = cs.ReplicaSet
	if fsm.SetName != "" {
		fsm.Kind = description.ReplicaSetNoPrimary
	} else if len(cs.Hosts) == 1 {
		fsm.Kind = description.Single
	}

	for _, host := range cs.Hosts {
		fsm.Servers = append(fsm.Servers, description.Server{Addr: addr.Addr(host).Canonicalize()})
	}

	return fsm
}

func applyResponses(f *fsm, responses []response) error {
	for _, response := range responses {
		server := description.NewServer(addr.Addr(response.Host), response.IsMaster, result.BuildInfo{})
		_, err := f.apply(server)

		if err != nil {
			return err
		}
	}

	return nil
}

func runTest(t *testing.T, directory string, filename string) {
	filepath := path.Join(testsDir, directory, filename)
	content, err := ioutil.ReadFile(filepath)
	require.NoError(t, err)

	// Remove ".json" from filename.
	filename = filename[:len(filename)-5]
	testName := directory + "/" + filename + ":"

	t.Run(testName, func(t *testing.T) {
		var test testCase
		require.NoError(t, json.Unmarshal(content, &test))
		f := setUpFSM(t, test.URI)

		for _, phase := range test.Phases {
			err = applyResponses(f, phase.Responses)
			if phase.Outcome.Compatible == nil || *phase.Outcome.Compatible {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				continue
			}

			require.Equal(t, phase.Outcome.TopologyType, f.Kind.String())
			require.Equal(t, phase.Outcome.SetName, f.SetName)
			require.Equal(t, len(phase.Outcome.Servers), len(f.Servers))

			for address, server := range phase.Outcome.Servers {
				fsmServer, ok := f.Server(addr.Addr(address))
				require.True(t, ok)

				require.Equal(t, addr.Addr(address), fsmServer.Addr)
				require.Equal(t, server.SetName, fsmServer.SetName)

				// PossiblePrimary is only relevant to single-threaded drivers.
				if server.Type == "PossiblePrimary" {
					server.Type = "Unknown"
				}

				require.Equal(t, server.Type, fsmServer.Kind.String())
			}
		}
	})
}

// Test case for all SDAM spec tests.
func TestSDAMSpec(t *testing.T) {
	for _, subdir := range []string{"single", "rs", "sharded"} {
		for _, file := range testhelpers.FindJSONFilesInDir(t, path.Join(testsDir, subdir)) {
			runTest(t, subdir, file)
		}
	}
}
