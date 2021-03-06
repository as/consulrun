package api

import (
	"strings"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
	"github.com/as/consulrun/hashicorp/serf/coordinate"
	"github.com/pascaldekloe/goe/verify"
)

func TestAPI_CoordinateDatacenters(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	coord := c.Coordinate()
	retry.Run(t, func(r *retry.R) {
		datacenters, err := coord.Datacenters()
		if err != nil {
			r.Fatal(err)
		}

		if len(datacenters) == 0 {
			r.Fatalf("Bad: %v", datacenters)
		}
	})
}

func TestAPI_CoordinateNodes(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	coord := c.Coordinate()
	retry.Run(t, func(r *retry.R) {
		_, _, err := coord.Nodes(nil)
		if err != nil {
			r.Fatal(err)
		}

	})
}

func TestAPI_CoordinateNode(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	coord := c.Coordinate()
	retry.Run(t, func(r *retry.R) {
		_, _, err := coord.Node(s.Config.NodeName, nil)
		if err != nil && !strings.Contains(err.Error(), "Unexpected response code: 404") {
			r.Fatal(err)
		}

	})
}

func TestAPI_CoordinateUpdate(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	node := "foo"
	_, err := c.Catalog().Register(&CatalogRegistration{
		Node:    node,
		Address: "1.1.1.1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	coord := c.Coordinate()
	newCoord := coordinate.NewCoordinate(coordinate.DefaultConfig())
	newCoord.Height = 0.5
	entry := &CoordinateEntry{
		Node:  node,
		Coord: newCoord,
	}
	_, err = coord.Update(entry, nil)
	if err != nil {
		t.Fatal(err)
	}

	retryer := &retry.Timer{Timeout: 5 * time.Second, Wait: 1 * time.Second}
	retry.RunWith(retryer, t, func(r *retry.R) {
		coords, _, err := coord.Node(node, nil)
		if err != nil {
			r.Fatal(err)
		}
		if len(coords) != 1 {
			r.Fatalf("bad: %v", coords)
		}
		verify.Values(r, "", coords[0], entry)
	})
}
