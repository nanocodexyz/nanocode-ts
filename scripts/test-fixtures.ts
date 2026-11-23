import * as Fs from 'node:fs'
import * as Path from 'node:path'
import * as Wasm from './wasm.mjs'

export function validate(options: validate.Options = {}): void {
  const directory = options.directory ?? Path.join(Wasm.root, 'dist')
  const receipt = options.receipt ?? Path.join(directory, Wasm.manifestName)
  const recorded: unknown = JSON.parse(Fs.readFileSync(receipt, 'utf8'))
  if (
    typeof recorded !== 'object' ||
    recorded === null ||
    !('fingerprint' in recorded) ||
    recorded.fingerprint !== Wasm.fingerprint()
  )
    throw new Error('The test WASM receipt does not match the current source fingerprint')

  const bytes = Fs.readFileSync(Path.join(directory, 'nanocode_wasm.wasm'))
  if (
    !('files' in recorded) ||
    typeof recorded.files !== 'object' ||
    recorded.files === null ||
    !('nanocode_wasm.wasm' in recorded.files) ||
    recorded.files['nanocode_wasm.wasm'] !== Wasm.sha256(bytes)
  )
    throw new Error('The test WASM module does not match its receipt')
}

export declare namespace validate {
  type Options = {
    directory?: string | undefined
    receipt?: string | undefined
  }
}
