import React, { useEffect, useRef } from 'react'
import NewsPage from './NewsPage'

const COMPETITOR_COPY = /Bloomberg\s+style/gi

function cleanNodeText(root) {
  if (!root || typeof document === 'undefined') return
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT)
  const nodes = []
  while (walker.nextNode()) nodes.push(walker.currentNode)
  nodes.forEach(node => {
    if (COMPETITOR_COPY.test(node.nodeValue || '')) {
      node.nodeValue = String(node.nodeValue || '').replace(COMPETITOR_COPY, 'Real-time')
    }
    COMPETITOR_COPY.lastIndex = 0
  })
}

export default function NewsPageClean() {
  const rootRef = useRef(null)

  useEffect(() => {
    const root = rootRef.current
    cleanNodeText(root)
    if (!root || typeof MutationObserver === 'undefined') return undefined
    const observer = new MutationObserver(() => cleanNodeText(root))
    observer.observe(root, { childList: true, subtree: true, characterData: true })
    return () => observer.disconnect()
  }, [])

  return <div ref={rootRef}><NewsPage /></div>
}
