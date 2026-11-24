import '@a2ui/lit'
import { renderMarkdown } from '@a2ui/markdown-it'
import { MessageProcessor } from '@a2ui/web_core'
import { basicCatalog, setMarkdownRenderer } from '@a2ui/web_core/catalogs/basic/v1'

const MESSAGES = 'api/messages'
const ACTIONS = 'api/actions'
const REFRESH_MILLISECONDS = 15000
const board = document.getElementById('board')
const status = document.getElementById('status')

setMarkdownRenderer((value, options) => renderMarkdown(value, options))

async function act(action) {
  status.textContent = `Sending ${action.name}…`
  const response = await fetch(ACTIONS, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ version: '1.0', action }),
  })
  status.textContent = response.ok
    ? `The maintainer takes ${action.name} on its next poll.`
    : `${response.status} ${response.statusText}`
}

function shown(messages) {
  const processor = new MessageProcessor([basicCatalog], act)
  processor.processMessages(messages)
  return [...processor.getSurfaces().values()].map((surface) =>
    Object.assign(document.createElement('a2ui-surface'), { surface }),
  )
}

async function refresh() {
  const response = await fetch(MESSAGES, { cache: 'no-store' })
  if (!response.ok) {
    status.textContent = `The maintainer does not answer: ${response.status} ${response.statusText}`
    return
  }
  board.replaceChildren(...shown(await response.json()))
  status.textContent = `Updated ${new Date().toLocaleTimeString()}`
}

function shownRefresh() {
  refresh().catch((error) => {
    status.textContent = `The maintainer does not answer: ${error.message}`
  })
}

shownRefresh()
setInterval(shownRefresh, REFRESH_MILLISECONDS)
