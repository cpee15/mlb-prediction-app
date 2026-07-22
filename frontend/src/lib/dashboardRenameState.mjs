export const DASHBOARD_NAME_MAX_LENGTH = 255

export function normalizeDashboardName(value) {
  const name = String(value || '').trim()
  if (!name) throw new Error('Name is required.')
  if (name.length > DASHBOARD_NAME_MAX_LENGTH) {
    throw new Error(`Name must be ${DASHBOARD_NAME_MAX_LENGTH} characters or fewer.`)
  }
  return name
}

export function dashboardRenameRequest(kind, id, value) {
  const name = normalizeDashboardName(value)
  if (!Number.isInteger(Number(id)) || Number(id) <= 0) throw new Error('A saved record is required.')
  if (kind === 'folder') {
    return {
      path: `/my-dashboard/folders/${id}`,
      options: {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder_name: name }),
      },
      name,
    }
  }
  if (kind === 'item') {
    return {
      path: `/my-dashboard/items/${id}`,
      options: {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: name }),
      },
      name,
    }
  }
  throw new Error('Unsupported saved record type.')
}

export function renameKeyboardAction(key) {
  if (key === 'Enter') return 'save'
  if (key === 'Escape') return 'cancel'
  return null
}
