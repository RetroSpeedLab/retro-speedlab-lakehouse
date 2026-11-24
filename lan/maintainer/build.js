import { build } from 'esbuild'

// A2UI 0.16 is written against zod 3; the patched zod 4.6.6 still carries that API at zod/v3.
const zodThree = {
  name: 'zod-v3',
  setup(bundler) {
    bundler.onResolve({ filter: /^zod$/ }, (found) =>
      bundler.resolve('zod/v3', { kind: found.kind, resolveDir: found.resolveDir }),
    )
  },
}

await build({
  entryPoints: ['src/main.js'],
  bundle: true,
  format: 'esm',
  minify: true,
  legalComments: 'eof',
  outfile: '../tools/maintainer/a2ui.js',
  plugins: [zodThree],
})
