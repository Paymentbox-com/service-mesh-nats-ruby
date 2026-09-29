# Development

```
mise install
just install
just check      # lint, test, build
```

## Tests

```
just test
```

Integration specs start a real `nats-server` on a random loopback port per
example group. The binary is found on `PATH` or at `NATS_SERVER_BIN`.
`spec/conformance_spec.rb` runs the `service_mesh` gem's shared examples
against this transport; the other specs cover what is NATS-specific.
