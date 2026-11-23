// workerd ships no types; its main export is the path of the workerd binary.
declare module 'workerd' {
  const path: string
  export default path
}
