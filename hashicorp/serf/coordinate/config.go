package coordinate

//
//
type Config struct {
	Dimensionality uint

	VivaldiErrorMax float64

	VivaldiCE float64

	VivaldiCC float64

	AdjustmentWindowSize uint

	HeightMin float64

	LatencyFilterSize uint

	GravityRho float64
}

func DefaultConfig() *Config {
	return &Config{
		Dimensionality:       8,
		VivaldiErrorMax:      1.5,
		VivaldiCE:            0.25,
		VivaldiCC:            0.25,
		AdjustmentWindowSize: 20,
		HeightMin:            10.0e-6,
		LatencyFilterSize:    3,
		GravityRho:           150.0,
	}
}
