# Task runner for service_mesh_nats. Run `just` with no arguments to see the menu.
#
# Every Ruby recipe runs through `mise exec` so it uses the Ruby pinned in
# mise.toml without relying on the shell's mise activation. If you run just from
# a bare environment where `mise` is not on PATH, change this to
# "/opt/homebrew/bin/mise exec -- bundle".
bundle := "mise exec -- bundle"

# The version in the gemspec, which names the built gem file
version := `mise exec -- ruby -e 'print Gem::Specification.load("service_mesh_nats.gemspec").version'`

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

# Build the gem into pkg/service_mesh_nats-<version>.gem
[group('build')]
build:
    mkdir -p pkg
    mise exec -- gem build service_mesh_nats.gemspec --output pkg/service_mesh_nats-{{version}}.gem

# Push the built gem to rubygems.org; asks for the MFA code
[group('release')]
publish:
    mise exec -- gem push pkg/service_mesh_nats-{{version}}.gem

# Tag the current commit v<version> and push the tag; refuses a working tree with changes
[group('release')]
tag:
    test -z "$(git status --porcelain)" || (echo "commit or stash your changes first" && exit 1)
    git tag -a v{{version}} -m "v{{version}}"
    git push origin v{{version}}

# Tag the current commit, build the gem, and push it to rubygems.org
[group('release')]
release: tag build publish

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
