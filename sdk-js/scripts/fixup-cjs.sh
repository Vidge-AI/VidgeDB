#!/bin/sh
# Post-build: mark dist/cjs as CommonJS. tsc emits plain .js there; without
# this marker, the top-level "type": "module" would misread them as ESM.
echo '{"type": "commonjs"}' > dist/cjs/package.json
echo "cjs package.json written"