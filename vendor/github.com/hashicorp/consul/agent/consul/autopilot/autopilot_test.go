package autopilot

import (
	"errors"
	"net"
	"testing"

	"github.com/hashicorp/serf/serf"
)

func TestMinRaftProtocol(t *testing.T) {
	t.Parallel()
	makeMember := func(version string) serf.Member {
		return serf.Member{
			Name: "foo",
			Addr: net.IP([]byte{127, 0, 0, 1}),
			Tags: map[string]string{
				"role":     "consul",
				"dc":       "dc1",
				"port":     "10000",
				"vsn":      "1",
				"raft_vsn": version,
			},
			Status: serf.StatusAlive,
		}
	}

	cases := []struct {
		members  []serf.Member
		expected int
		err      error
	}{

		{
			members:  []serf.Member{},
			expected: -1,
			err:      errors.New("No servers found"),
		},

		{
			members: []serf.Member{
				makeMember("1"),
			},
			expected: 1,
		},

		{
			members: []serf.Member{
				makeMember("asdf"),
			},
			expected: -1,
			err:      errors.New(`strconv.Atoi: parsing "asdf": invalid syntax`),
		},

		{
			members: []serf.Member{
				makeMember("1"),
				makeMember("2"),
			},
			expected: 1,
		},

		{
			members: []serf.Member{
				makeMember("2"),
				makeMember("2"),
			},
			expected: 2,
		},
	}

	serverFunc := func(m serf.Member) (*ServerInfo, error) {
		return &ServerInfo{}, nil
	}
	for _, tc := range cases {
		result, err := minRaftProtocol(tc.members, serverFunc)
		if result != tc.expected {
			t.Fatalf("bad: %v, %v, %v", result, tc.expected, tc)
		}
		if tc.err != nil {
			if err == nil || tc.err.Error() != err.Error() {
				t.Fatalf("bad: %v, %v, %v", err, tc.err, tc)
			}
		}
	}
}
