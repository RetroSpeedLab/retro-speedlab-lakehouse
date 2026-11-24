import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'

const PORT = 8095
const PAGE = new URL('../tools/maintainer/', import.meta.url)
const FILES = {
  '/': ['index.html', 'text/html'],
  '/a2ui.js': ['a2ui.js', 'text/javascript'],
}

createServer(async (request, response) => {
  if (request.method === 'POST' && request.url === '/api/actions') {
    request.pipe(process.stdout)
    response.writeHead(202, { 'Content-Type': 'application/json' }).end('{}')
    return
  }
  if (request.url === '/api/messages') {
    response
      .writeHead(200, { 'Content-Type': 'application/json' })
      .end(await readFile(new URL('fixture.json', import.meta.url)))
    return
  }
  if (!(request.url in FILES)) {
    response.writeHead(404).end()
    return
  }
  const [file, type] = FILES[request.url]
  response.writeHead(200, { 'Content-Type': type }).end(await readFile(new URL(file, PAGE)))
}).listen(PORT, () => console.log(`The maintainer view with its fixture: http://localhost:${PORT}/`))
