Contribution Guide
==================

Coding Standards
-----------------

You must apply the latest `rustfmt` (formtter) and `clippy` (linter) as follows:

```console
$ cargo fmt --all
$ cargo clippy
```

If any warnings are emitted, you need to fix them.

For other coding styles that can not be covered with the above tools, follow [Rust's Style Guildelines] as much as possible.

[Rust's Style Guildelines]: https://doc.rust-lang.org/1.0.0/style/README.html


Pull Request
------------

Before creating a pull request, please make sure that the unit tests pass in your environment:

```console
$ cargo test
```


Versioning
----------

This project adopts [Semantic Versioning].

[Semantic Versioning]: https://semver.org/
