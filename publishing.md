# Publishing

`service_mesh_nats` is published to [rubygems.org](https://rubygems.org/gems/service_mesh_nats) by hand
from a local checkout. The account that pushes is an owner of the gem, and
rubygems.org asks for its MFA code on every push because the gemspec sets
`rubygems_mfa_required`.

This gem depends on `service_mesh`. A release that needs a newer `service_mesh` is published after that `service_mesh` version is on rubygems.org, since `gem push` accepts the gem either way but `bundle install` fails for anyone installing it until the dependency exists.

## One-time setup

```sh
gem signin
```

This stores an API key in `~/.gem/credentials`.

## Releasing a version

1. Set the new version in `lib/service_mesh_nats/version.rb` (`ServiceMeshNats::VERSION`), commit, and push `master`.
2. Wait for CI on `master` to pass.
3. Tag the commit and push the tag:

   ```sh
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

4. Build the gem from the tagged commit and push it:

   ```sh
   git checkout vX.Y.Z
   git status --short          # must print nothing
   mise exec -- gem build service_mesh_nats.gemspec
   mise exec -- gem push service_mesh_nats-X.Y.Z.gem
   git checkout master
   ```

   `gem push` prompts for the MFA code.

A pushed version is permanent. It can be yanked with `gem yank service_mesh_nats -v X.Y.Z`,
but that version number can never be pushed again.

## Adding an owner

```sh
gem owner service_mesh_nats --add someone@example.com
```
