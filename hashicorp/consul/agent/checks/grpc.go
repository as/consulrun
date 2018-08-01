package checks

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

var ErrGRPCUnhealthy = fmt.Errorf("gRPC application didn't report service healthy")

type GrpcHealthProbe struct {
	server      string
	request     *hv1.HealthCheckRequest
	timeout     time.Duration
	dialOptions []grpc.DialOption
}

func NewGrpcHealthProbe(target string, timeout time.Duration, tlsConfig *tls.Config) *GrpcHealthProbe {
	serverAndService := strings.SplitN(target, "/", 2)

	server := serverAndService[0]
	request := hv1.HealthCheckRequest{}
	if len(serverAndService) > 1 {
		request.Service = serverAndService[1]
	}

	var dialOptions = []grpc.DialOption{}

	if tlsConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	return &GrpcHealthProbe{
		server:      server,
		request:     &request,
		timeout:     timeout,
		dialOptions: dialOptions,
	}
}

func (probe *GrpcHealthProbe) Check() error {
	ctx, cancel := context.WithTimeout(context.Background(), probe.timeout)
	defer cancel()

	connection, err := grpc.DialContext(ctx, probe.server, probe.dialOptions...)
	if err != nil {
		return err
	}
	defer connection.Close()

	client := hv1.NewHealthClient(connection)
	response, err := client.Check(ctx, probe.request)
	if err != nil {
		return err
	}
	if response == nil || (response != nil && response.Status != hv1.HealthCheckResponse_SERVING) {
		return ErrGRPCUnhealthy
	}

	return nil
}
