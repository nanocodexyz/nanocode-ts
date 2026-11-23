import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { Bash } from '../src/bash/index.ts'

async function run(script: string): Promise<string> {
  const bash = await Bash.create()
  const result = await bash.exec(script)

  assert.equal(result.stderr, '', script)
  assert.equal(result.exitCode, 0, script)
  return result.stdout
}

describe('jq offsets in code points', () => {
  test('match after one emoji', async () => {
    assert.equal(
      await run(`jq -nc '"😀a" | match("a")'`),
      '{"offset":1,"length":1,"string":"a","captures":[]}\n',
    )
  })

  test('match over an astral character', async () => {
    assert.equal(await run(`jq -nc '"x😀y" | match("😀y") | [.offset, .length]'`), '[1,2]\n')
  })

  test('global match offsets', async () => {
    assert.equal(await run(`jq -nc '[ "😀a𝄞a" | match("a"; "g") | .offset ]'`), '[1,3]\n')
  })

  test('capture offsets and named captures', async () => {
    assert.equal(
      await run(`jq -nc '"😀😀ab" | match("(?<x>b)") | .captures[0] | [.offset, .length, .name]'`),
      '[3,1,"x"]\n',
    )
    assert.equal(await run(`jq -nc '"😀😀ab" | capture("(?<x>a)(?<y>b)")'`), '{"x":"a","y":"b"}\n')
  })

  test('test on astral input', async () => {
    assert.equal(await run(`jq -nc '"😀a" | test("^.a$")'`), 'true\n')
  })

  test('split, sub and gsub keep astral characters', async () => {
    assert.equal(await run(`jq -nc '"😀a😀a" | gsub("a"; "b")'`), '"😀b😀b"\n')
    assert.equal(await run(`jq -nc '"😀-𝄞" | split("-"; null)'`), '["😀","𝄞"]\n')
    assert.equal(await run(`jq -nc '"😀a" | sub("a"; "b")'`), '"😀b"\n')
  })

  test('length of an astral string', async () => {
    assert.equal(await run(`jq -nc '"😀𝄞a" | length'`), '3\n')
  })
})

describe('yq', () => {
  test('match after one emoji', async () => {
    assert.equal(
      await run(
        `printf 'name: "😀a"\\n' | yq -o json -I 0 '.name | match("a") | [.offset, .length]'`,
      ),
      '[1,1]\n',
    )
  })

  test('reads a field', async () => {
    assert.equal(await run(`printf 'a:\\n  b: [1, 2]\\n' | yq '.a.b[1]'`), '2\n')
  })

  test('converts json to yaml', async () => {
    assert.equal(await run(`echo '{"k":"v"}' | yq -p json '.'`), 'k: v\n')
  })
})

describe('declare -f', () => {
  test('prints a function', async () => {
    assert.equal(
      await run(`f() { echo "😀[a-b]/\${x}"; }; declare -f f`),
      'f () \n{ \n    echo "😀[a-b]/${x}"\n}\n',
    )
  })

  test('lists functions with -F', async () => {
    assert.equal(await run(`b() { :; }; a() { :; }; declare -F`), 'declare -f a\ndeclare -f b\n')
  })
})
