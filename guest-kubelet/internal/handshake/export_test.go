package handshake

// WithDefaults exposes withDefaults to the tests in package handshake_test.
func (o Options) WithDefaults() Options { return o.withDefaults() }
