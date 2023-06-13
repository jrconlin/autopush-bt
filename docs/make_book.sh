#!/bin/bash
echo Generating the cargo docs
cargo doc --all-features --workspace --no-deps
echo Generating mdbook
mdbook build
mkdir -p ./output/api
cp -r ../target/doc/* ./output/api
