#!/usr/bin/env bash

curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
echo $HOME/.bashrc >> "source $HOME/.cargo/env"
rustup install stable
rustup default stable
rustup install nightly
cargo +nightly install racer
rustup component add rust-src
rustup component add clippy-preview
rustup component add rustfmt-preview
