# Task runner for service_mesh_nats. Run `just` with no arguments to see the menu.
#
# Every Ruby recipe runs through `mise exec` so it uses the Ruby pinned in
# mise.toml without relying on the shell's mise activation. If you run just from
# a bare environment where `mise` is not on PATH, change this to
# "/opt/homebrew/bin/mise exec -- bundle".
bundle := "mise exec -- bundle"

# List all recipes
default:
    @just --list

# Install gem dependencies
[group('build')]
install:
    {{bundle}} install

# Run the full test suite (needs nats-server on PATH)
[group('build')]
test:
    {{bundle}} exec rspec

# Build the gem into pkg/
[group('build')]
build:
    mkdir -p pkg
    mise exec -- gem build service_mesh_nats.gemspec --output pkg/service_mesh_nats.gem

# Report lint findings (matches CI)
[group('checks')]
lint:
    {{bundle}} exec rubocop

# Fix lint findings in place
[group('checks')]
fmt:
    {{bundle}} exec rubocop -A

# Everything CI checks, in the order CI runs them
[group('checks')]
check: lint test build
