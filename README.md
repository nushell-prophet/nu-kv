# File based nushell kv-module

- Why does this module exist, as we already have the fine `std-rfc/kv` module?
- I started developing and using this module a long time ago, and my scripts already use it. Additionally, this module relies on version-control friendly local files, rather than on an SQLite database, which also provides some advantages for me. So I continue its development.

## Features

- Autocompletion for key names
- Ability to set folders to save objects (via `$env.kv.path` or by using the `kv set --cwd` flag)
- Ability to alter variable names using `$env.kv.keys-suffix` (useful for debugging modules inside bulk tests)
- Timestamped history of all versions of objects

## Acknowledgment

The initial version of this module was created by `@clipplerblood` and shared on the [discord](https://discord.com/channels/601130461678272522/615253963645911060/1149709351821516900).
