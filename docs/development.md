# Development

This page is for working on the transport itself. It covers the recipes, the
tests, and releasing a version.

## Recipes

```
mise install
just install
just check      # lint, test, build
```

Tool versions are pinned in `mise.toml`. `just` with no arguments lists the
recipes.

| Recipe | What it does |
|---|---|
| `just install` | Installs the gem's dependencies with Bundler. |
| `just test` | Runs the test suite. It needs `nats-server`. |
| `just build` | Builds the gem into `pkg/service_mesh_nats-<version>.gem`. |
| `just lint` | Reports lint findings with RuboCop. |
| `just fmt` | Fixes lint findings in place with RuboCop. |
| `just check` | Runs `lint`, `test`, and `build`, in the order CI runs them. |
| `just tag` | Tags the current commit with the gem's version and pushes the tag. It refuses a working tree with changes. |
| `just publish` | Pushes the built gem to rubygems.org. |
| `just release` | Runs `tag`, `build`, and `publish`. |
| `just bump patch`, `just bump minor`, `just bump major` | Raises the version in `lib/service_mesh_nats/version.rb` by one step. A minor bump resets the patch number, and a major bump resets both. |

## Tests

The integration specs start a real `nats-server` on a random loopback port for
each example group. The binary is found on `PATH` or at `NATS_SERVER_BIN`.

`spec/conformance_spec.rb` runs the `service_mesh` gem's shared examples against
this transport, and the other specs cover what is specific to NATS.

## Releasing

Publishing to rubygems.org is described in [publishing.md](../publishing.md).
